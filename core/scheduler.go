package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"sync"

	"github.com/aodr3w/keiji-bus-client/client"
	"github.com/aodr3w/keiji-core/db"
	"github.com/aodr3w/keiji-core/paths"
	"github.com/aodr3w/keiji-core/utils"
	"github.com/aodr3w/logger"
	"github.com/joho/godotenv"
)

/*
stopChanData encapsulates all the signal data
that can be sent to a running task
*/
type stopChanData struct {
	disable bool
	stop    bool
	delete  bool
}

/*
Executor type provides access to primitives and
functions that facilitate concurrent execution of tasks
*/
type Executor struct {
	repo       *db.Repo
	logger     *logger.Logger
	stopChans  map[string]chan stopChanData
	tasksQueue chan *db.TaskModel
	ctx        context.Context
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
}

/*
days map , maps string `day values` to time.Day values
*/
var days = map[string]int64{
	"Sunday":    int64(time.Sunday),
	"Monday":    int64(time.Monday),
	"Tuesday":   int64(time.Tuesday),
	"Wednesday": int64(time.Wednesday),
	"Thursday":  int64(time.Thursday),
	"Friday":    int64(time.Friday),
	"Saturday":  int64(time.Saturday),
}

/*
NewExecutor is a factory function for the Executor type
*/
func NewExecutor() (*Executor, error) {
	repo, err := db.NewRepo()
	if err != nil {
		return nil, err
	}
	logger, err := logger.NewFileLogger(paths.SCHEDULER_LOGS)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Executor{
		repo:       repo,
		logger:     logger,
		stopChans:  make(map[string]chan stopChanData),
		tasksQueue: make(chan *db.TaskModel, 100),
		ctx:        ctx,
		cancel:     cancel,
		wg:         &sync.WaitGroup{},
	}, nil
}

/*
executor.Stop function gracefully stops the executor instance,
by propagating a stop singal via context (to cancel running tasks)
and closing the executor's db session via e.repo.Close()
*/
func (e *Executor) Stop() {
	e.logger.Warn("stopping executor...")
	e.markAllTasksAsNotRunning()
	e.cancel()
	e.wg.Wait()
	//then close repo
	e.repo.Close()
}

/*
dayNum function returns an int64 representation of day string
if valid otherwise  -1 and an error are returned
*/
func (e *Executor) DayNum(day string) (int64, error) {
	num, ok := days[day]
	if !ok {
		return -1, fmt.Errorf("invalid day value %v", day)
	}
	return num, nil
}

/*
markAllTasksAsNotRunning as its name suggests
sets all tasks to is_running = false. This function is called
during the shutdown of an executor
*/
func (e *Executor) markAllTasksAsNotRunning() {
	tasks, err := e.repo.GetRunningTasks()
	if err != nil {
		e.logger.Error("failed to get running tasks: %v", err)
		return
	}

	e.logger.Info("updating running tasks...")
	for _, task := range tasks {
		if _, err := e.repo.SetIsRunning(task.Slug, false); err != nil {
			e.logger.Error("failed to mark task %v as not running: %v", task.Slug, err)
		} else {
			e.logger.Info("task updated before shutdown")
		}
	}
}

/*
The start function starts the executor by peforming three actions concurrently
* loading tasks into a queue
* reading and running each task from the queue
* listening to a TCP bus for new messages like stop and restart signals
*/
func (e *Executor) Start() {
	//load tasks from db
	go func() {
		delay := 2 * time.Second
		for {
			select {
			case <-e.ctx.Done():
				e.logger.Info("stopping load tasks...")
				e.logger.Info("closing executor.tasksQueue...")
				close(e.tasksQueue)
				return
			default:
				err := e.LoadTasks()
				if err != nil {
					break
				}
				time.Sleep(delay)
			}
		}
	}()

	//run tasks
	e.wg.Add(1)
	go func() {
		e.RunTasks()
		defer e.wg.Done()
	}()

	//listen to tcp-bus pull port for new messages
	go func() {
		e.logger.Info("starting tcp-bus listener")
		e.listenToBus()
	}()

}

/*
listenToBus function listens for new messages on the pull port of the tcp-bus service
*/
func (e *Executor) listenToBus() {
	for {
		select {
		case <-e.ctx.Done():
			e.logger.Info("stopping tcp-bus listener")
			return
		default:
			time.Sleep(100 * time.Millisecond)
			conn, err := net.Dial("tcp", client.PULL_PORT)
			if err != nil {
				e.logger.Error("%v", err)
				continue
			}
			data, err := io.ReadAll(conn)
			if err != nil && !errors.Is(err, io.EOF) {
				e.logger.Error("%v", err)
				conn.Close()
				continue
			}
			//read message if any
			var message client.Message
			err = json.Unmarshal(data, &message)

			if err != nil {
				e.logger.Error("%v", err)
				conn.Close()
				continue
			}
			//handle message
			go e.HandleMessage(&message)
			//repeat
			conn.Close()
		}
	}
}

/*
handleMessage handles messages sent on the tcp-bus and
translates them into disable / stop / delete signals for tasks
*/
func (e *Executor) HandleMessage(msg *client.Message) {
	e.logger.Info("handling message...")
	cmd, ok := (*msg)["cmd"]
	if !ok {
		e.logger.Error("cmd not found in message: %v", msg)
		return
	}

	taskID, ok := (*msg)["taskID"]

	if !ok {
		e.logger.Error("taskID not provided for startTask command: %v", msg)
		return
	}

	stopChan, err := e.getStopChan(taskID)
	if err != nil {
		e.logger.Error("%v", err)
		return
	}

	switch cmd {
	case "disable":
		stopChan <- stopChanData{
			disable: true,
		}
	case "stop":
		stopChan <- stopChanData{
			stop: true,
		}
	case "delete":
		stopChan <- stopChanData{
			delete: true,
		}

	default:
		e.logger.Error("cannot handle message %v", cmd)

	}

}

func (e *Executor) getStopChan(taskId string) (chan stopChanData, error) {
	stopChan, ok := e.stopChans[taskId]
	if !ok {
		return nil, fmt.Errorf("stop channel not found for task %v", taskId)
	}
	return stopChan, nil
}

/*
LoadTasks retrieves runnable tasks from the database,
sets status to queued and
pushes each task to  a tasksQueue channel
*/
func (e *Executor) LoadTasks() error {
	e.repo.ResetIsQueued()
	allTasks, err := e.repo.GetRunnableTasks()
	if err != nil {
		return err
	}
	for idx := range allTasks {
		task := allTasks[idx]
		//set is queued to true to avoid enquing the same task since this function runs in a loop
		task, err = e.repo.SetIsQueued(task.Slug, true)
		if err != nil {
			e.logger.Error("setIsQueued failed for task %v , err: %v", task.Slug, err)
		} else {
			e.tasksQueue <- task
		}
	}
	return nil
}

/*
RunTasks reads tasks from the taskQueue
and runs each task in a seperate goroutine depending.
*/
func (e *Executor) RunTasks() {
	e.logger.Info("running start function")
	for {
		select {
		case <-e.ctx.Done():
			e.logger.Info("stopping run tasks...")
			return
		case task := <-e.tasksQueue:
			stopChan := make(chan stopChanData, 1)
			e.stopChans[task.TaskId] = stopChan
			e.wg.Add(1)
			go func(task *db.TaskModel, stopChan chan stopChanData) {
				defer e.wg.Done()
				//move for select here
				switch task.Type {
				case db.TaskType(db.HMSTask):
					e.RunHMSTask(task)
				case db.DayTime:
					e.RunDayTimeTask(task)
				default:
					e.logger.Error("invalid task type")
					return
				}
			}(task, stopChan)
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

}

func (e *Executor) deleteTaskExecutable(executable string) error {
	dir := filepath.Dir(executable)
	runFile := filepath.Join(dir, fmt.Sprintf("%v_run.bin", strings.ReplaceAll(filepath.Base(executable), ".bin", "")))
	fmt.Printf("deleting %v, %v\n", executable, runFile)
	for _, f := range []string{executable, runFile} {
		err := os.Remove(f)
		if err != nil {
			return err
		}
	}
	return nil
}
func (e *Executor) deleteTaskLog(taskId string) error {
	task_obj, err := e.repo.GetTaskByID(taskId)
	if err != nil {
		return err
	}
	logsPath := task_obj.LogPath
	exists, err := utils.DirectoryExists(logsPath)
	if err != nil {
		e.logger.Error("%v", err)
		return err
	}
	if !exists {
		err = fmt.Errorf("logs Path %v not found for task %v", logsPath, taskId)
		e.logger.Error("%v", err)
		return err
	}
	return os.Remove(logsPath)
}

func (e *Executor) closeTaskChans(taskId string) {
	if stopChan, ok := e.stopChans[taskId]; !ok {
		e.logger.Error("failed to close stop channel for task %v not found", taskId)
	} else {
		close(stopChan)
		delete(e.stopChans, taskId)
	}
}

func (e *Executor) RunHMSTask(task *db.TaskModel) error {
	log, err := logger.NewFileLogger(fmt.Sprintf("%v/%v", paths.TASK_LOG, task.Slug))
	if err != nil {
		return err
	}
	scheduleInfo := task.ScheduleInfo
	interval, err := e.getInterval(task)
	if err != nil {
		err := fmt.Errorf("invalid interval value for task %v", task.TaskId)
		_, setErr := e.repo.SetIsError(task.Slug, true, err.Error())
		if setErr != nil {
			return setErr
		}
		return err
	}
	unit, err := e.getUnits(scheduleInfo)
	if err != nil {
		err := fmt.Errorf("invalid unit value for task %v", task.TaskId)
		_, setErr := e.repo.SetIsError(task.Slug, true, err.Error())
		if setErr != nil {
			return setErr
		}
		return err
	}
	var duration time.Duration
	switch unit {
	case "seconds":
		duration = time.Duration(interval) * time.Second
	case "minutes":
		duration = time.Duration(interval) * time.Minute
	case "hours":
		duration = time.Duration(interval) * time.Hour
	default:
		err := fmt.Errorf("invalid unit %v for HMS task must be seconds, minutes or hours", unit)
		_, setErr := e.repo.SetIsError(task.Slug, true, err.Error())
		if setErr != nil {
			return setErr
		}
		return err
	}

	e.executeTask(
		task,
		log,
		duration,
	)

	return nil
}

func (e *Executor) tz() string {
	timezone := "Africa/Nairobi"
	err := godotenv.Load(paths.WORKSPACE_SETTINGS)
	if err != nil {
		log.Printf("failed to load env due to error %v\n", err)
	} else {
		tz := os.Getenv("tz")
		if len(tz) > 0 {
			tz = timezone
			log.Printf("timezone set to:  %v\n", tz)
		}
	}
	return timezone
}

func (e *Executor) executeTask(task *db.TaskModel, logger *logger.Logger, duration time.Duration) {
	ticker := time.NewTicker(duration)
	defer ticker.Stop()
	stopChan, stopChanFound := e.stopChans[task.TaskId]
	if !stopChanFound {
		e.logger.Error("stop channel for task %v not found", task.Slug)
		return
	}

	executable := task.Executable

	ok, err := utils.DirectoryExists(executable)

	if err != nil || !ok {
		if err == nil {
			err = fmt.Errorf("executable directory does not exist")
		}
		logger.Error("%v", err)
		_, err = e.repo.SetIsError(task.Slug, true, err.Error())
		if err != nil {
			logger.Error(err.Error())
		}
		return
	}
	//copy here
	executable, err = e.copyBinary(executable)
	if err != nil {
		e.logger.Error("%v", err)
		return
	}
	if !ok {
		logger.Error("%v", err)
		_, err := e.repo.SetIsError(task.Slug, true, fmt.Sprintf("Executable not found at %v", executable))
		if err != nil {
			logger.Error(err.Error())
		}
		return
	}

	task, err = e.repo.SetIsRunning(task.Slug, true)

	if err != nil {
		e.logger.Error("%v", err)
		e.closeTaskChans(task.TaskId)
		return
	}
	for {
		select {
		case <-e.ctx.Done():
			e.logger.Info("[ system shutdown ] terminating task: %v", task.TaskId)
			e.closeTaskChans(task.TaskId)
			return

		case stopSig := <-stopChan:
			if stopSig.disable {
				_, err := e.repo.SetIsDisabled(task.Slug, true)
				if err != nil {
					e.logger.Error("setIsDisabled Error: %v", err)
				} else {
					e.logger.Info("task %v disabled successfully", task.Slug)
					e.closeTaskChans(task.TaskId)
					return
				}
			} else if stopSig.stop {
				e.logger.Info("[ stop task ] terminating task... %v", task.TaskId)
				//set is running to false
				_, err := e.repo.SetIsRunning(task.Slug, false)
				if err != nil {
					e.logger.Error("%v", err)
				} else {
					e.logger.Info("task stopped successfully")
					e.closeTaskChans(task.TaskId)
					return
				}
			} else if stopSig.delete {
				err = e.deleteTaskLog(task.TaskId)
				if err != nil {
					e.logger.Error("delete logsPath error %v ", err)
				} else {
					e.logger.Info("task %v logs file deleted ", task.TaskId)
				}
				err = e.deleteTaskExecutable(task.Executable)
				//delete binaries at executable path
				if err != nil {
					e.logger.Error("delete executables error: %v", err)
				} else {
					e.logger.Info("task %v executables deleted", task.TaskId)
				}
				//delete record from db and return
				err := e.repo.DeleteTask(task)
				if err != nil {
					e.logger.Error("failed to delete task %v, err: %v", task.Slug, err)
				} else {
					e.logger.Info("task successfully deleted")
					e.closeTaskChans(task.TaskId)
					return
				}
			}

		case <-ticker.C:
			var execError error
			execError = e.runBinary(logger, task.Executable)
			if execError != nil {
				_, err := e.repo.SetIsError(task.Slug, true, execError.Error())
				if err != nil {
					logger.Error(err.Error())
				}
				return
			}
			//if the task is a DayTime task, re-compute wait time and recreate ticker
			if task.Type == db.DayTime {
				//determine next execution time based on scheduleInfo
				duration, err := e.getInterval(task)
				if err != nil {
					logger.Error("%v", err)
					e.closeTaskChans(task.TaskId)
					return
				}
				e.logger.Info("task %v next execution in %v seconds", task.TaskId, duration)
				ticker.Stop()
				ticker = time.NewTicker(time.Duration(duration))
			}
		}
	}

}

func (e *Executor) RunDayTimeTask(task *db.TaskModel) error {
	//get logger
	log, err := logger.NewFileLogger(fmt.Sprintf("%v/%v", paths.TASK_LOG, task.Slug))
	if err != nil {
		return err
	}
	//get schedule
	scheduleInfo := task.ScheduleInfo
	if scheduleInfo == nil {
		err := fmt.Errorf("schedule info for task %v was not found", task.Slug)
		e.closeTaskChans(task.TaskId)
		return err
	}
	//call e.execute
	duration, err := e.getInterval(task)
	if err != nil {
		log.Error("error determining task interval %v", err)
	}

	e.executeTask(
		task,
		log,
		time.Duration(duration),
	)

	return nil
}

func (e *Executor) copyBinary(path string) (string, error) {
	execDir := filepath.Dir(path)
	fileName := strings.ReplaceAll(filepath.Base(path), ".bin", "_run.bin")
	runPath := fmt.Sprintf("%v/%v", execDir, fileName)
	err := utils.CopyFile(path, runPath)
	if err != nil {
		return "", err
	}
	return runPath, nil
}

func (e *Executor) runBinary(logger *logger.Logger, path string, args ...string) error {
	cmd := exec.Command(path, args...)
	output, err := cmd.CombinedOutput()
	outputs := strings.Split(string(output), "\n")

	if err != nil {
		// Log each line of output separately
		for _, line := range outputs {
			if len(line) > 0 {
				logger.Error("%v", line)
			}
		}
		return fmt.Errorf("[ task error ]: %s", strings.Join(outputs, "\n"))
	}

	// Log each line of output separately
	for _, line := range outputs {
		if len(line) > 0 {
			logger.Info("%v", line)
		}
	}

	return nil
}

func (e *Executor) getInterval(task *db.TaskModel) (int64, error) {
	now := time.Now()
	if task.NextExecutionTime != nil && now.Before(*task.NextExecutionTime) {
		duration := int64(task.NextExecutionTime.Sub(now))
		if duration < 1 {
			duration = 1
		}
		return duration, nil
	}
	scheduleInfo := task.ScheduleInfo
	if scheduleInfo == nil {
		return 0, fmt.Errorf("scheduleInfo not provided for task %v", task.Slug)
	}
	if task.Type == db.TaskType(db.HMSTask) {
		e.logger.Info("getting interval for HMS task %v", task.Slug)
		value, ok := scheduleInfo["interval"].(float64)
		if !ok {
			e.logger.Info("not ok %v %v", value, ok)
			// If the value is not an int64, try converting it from a string
			strValue, strOk := scheduleInfo["interval"].(string)
			if !strOk {
				return 0, fmt.Errorf("failed to get interval from %v", scheduleInfo)
			}
			e.logger.Info("string Interval Value %v", strValue)
			intValue, err := strconv.Atoi(strValue)
			if err != nil {
				return -1, err
			}
			if intValue < 1 {
				return -1, fmt.Errorf("invalid interval value , must be >= 1")
			}
			e.logger.Info("extracted Interval Value %v", intValue)
			return int64(intValue), nil
		}
		e.logger.Info("Interval Value %v", value)
		return int64(value), nil
	} else if task.Type == db.HMSTask {

		e.logger.Info("getting interval for DayTime task %v", task.Slug)
		day, ok := scheduleInfo["day"]

		if !ok {
			err := fmt.Errorf("`Day` not found in DayTimeTask schedule for task: %v", task.Slug)
			e.logger.Error("%v", err)
			return -1, err
		}

		dayStr, ok := day.(string)

		if !ok {
			err := fmt.Errorf("`Day` value %v type is incorrect expected string for task: %v", dayStr, task.Slug)
			e.logger.Error("%v", err)
			return -1, err
		}

		e.logger.Info("[task-%v] dayStr: %v", task.Slug, dayStr)

		dayNum, err := e.DayNum(dayStr)

		if err != nil {
			err := fmt.Errorf("error getting DayNum: %v for task %v", err, task.Slug)
			e.logger.Error("%v", err)
			return -1, err
		}

		e.logger.Info("[task-%v] dayNum: %v", task.Slug, dayNum)

		t, ok := scheduleInfo["time"]

		if !ok {
			err := fmt.Errorf("time value not found for task %v", task.Slug)
			e.logger.Error("%v", err)
			return -1, err
		}

		tStr, ok := t.(string)

		if !ok {
			err := fmt.Errorf("`time` value %v type is incorrect expected string for task: %v", t, task.Slug)
			e.logger.Error("%v", err)
			return -1, err
		}

		e.logger.Info("[task-%v] timeStr: %v", task.Slug, tStr)

		pt, err := time.Parse("15:04", tStr)

		if err != nil {
			err := fmt.Errorf("failed to parse timeStr: %v due to error %v for task %v", tStr, err, task.Slug)
			e.logger.Error("%v", err)
			return -1, err
		}

		e.logger.Info("[task-%v] time: %v", task.Slug, pt)

		//load timezone info
		tz := e.tz()

		if len(tz) == 0 {
			err := fmt.Errorf("TIME_ZONE NOT SET")
			return -1, err
		}

		loc, err := time.LoadLocation(tz)
		if err != nil {
			err := fmt.Errorf("failed to load location: %v", err)
			e.logger.Error("%v", err)
			return -1, err
		}

		now := time.Now().In(loc)
		currentDay := int(now.Weekday())
		targetDay := int(dayNum)

		daysUntilTarget := ((targetDay - currentDay) + 7) % 7

		if daysUntilTarget == 0 {
			e.logger.Info("days until target == 0, now: %v, targetTime %v now < targetTime: %v", now, pt, now.Before(pt))
			nextExecutionTime := e.GetNextExecutonTime(now, pt, daysUntilTarget, loc)
			e.logger.Info("nextExecutionTime %v", nextExecutionTime)
			if now.Before(nextExecutionTime) {
				e.logger.Info("returning next Execution time")
				err = e.repo.UpdateExecutionTime(task, &nextExecutionTime, &now)
				if err != nil {
					return -1, err
				}
				return int64(time.Until(nextExecutionTime)), nil
			}
			daysUntilTarget = 7
		}

		e.logger.Info("[task-%v] days until next run: %v", task.Slug, daysUntilTarget)

		// nextExecutionTime := time.Date(now.Year(), now.Month(), now.Day(), pt.Hour(), pt.Minute(), 0, 0, loc).AddDate(0, 0, daysUntilTarget)
		nextExecutionTime := e.GetNextExecutonTime(now, pt, daysUntilTarget, loc)
		e.logger.Info("[task-%v] NextExecutionTime: %v", task.Slug, nextExecutionTime)
		duration := time.Until(nextExecutionTime)
		e.logger.Info("updating NextExecutionTime...")
		err = e.repo.UpdateExecutionTime(task, &nextExecutionTime, &now)
		e.logger.Info("updateExecutionTime: %v", err)
		if err != nil {
			return -1, err
		}
		return int64(duration), nil
	}

	return -1, fmt.Errorf("task has invalid task.Type name %v type %v", task.Slug, task.TaskId)

}

func (e *Executor) GetNextExecutonTime(from time.Time, target time.Time, daysUntilTarget int, loc *time.Location) time.Time {
	return time.Date(from.Year(), from.Month(), from.Day(), target.Hour(), target.Minute(), 0, 0, loc).AddDate(0, 0, daysUntilTarget)
}
func (e *Executor) getUnits(scheduleInfo map[string]interface{}) (string, error) {
	value, ok := scheduleInfo["units"].(string)
	if !ok {
		return "", fmt.Errorf("failed to get interval from %v", scheduleInfo)
	}
	if value != "seconds" && value != "minutes" && value != "hours" {
		return "", fmt.Errorf("invalid units value %v expected seconds, hours or minutes", value)
	}
	return value, nil
}
