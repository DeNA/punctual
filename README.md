# punctual [![Build Status](https://travis-ci.org/DeNA/punctual.png)](https://travis-ci.org/DeNA/punctual)
Redis-backed task queuing, polling, and processing mechanism for delayed task execution.  


## Description
Designed to be lightweight and highly customizable, this package provides a task queueing & polling mechanism as well as base task-related objects from which to inherit.

It manages non-local persistance through [Redis](http://redis.io) instead of a dedicated MQ.  This reduces complexity in the stack, as Redis is already a commonly used component for ephemeral storage & caching.     
 

### Installation


    npm install punctual


### Dependencies
Depends on
 * [Node.js](http://nodejs.org/) (>= v0.8.26)
 * [Redis](http://redis.io/) (tested w/ v2.4.17)



### How it's Different
Internally works a bit like [kue](https://github.com/LearnBoost/kue), storing scheduled tasks as [sorted-set](http://redis.io/commands#sorted_set) (ZSET) members and individual [hashes](http://redis.io/commands#hash).  Unlike [kue](https://github.com/LearnBoost/kue), however, it: 
 * distinguishes [TaskRunners](https://github.com/DeNA/punctual/blob/master/lib/TaskRunner.js) - TaskRunner prototypes can be programmed to handle & process specific task types  
 * does not implement a REST API, instead exposing a DAO where methods can be called imperatively
 * does not use 'priority' in task execution, instead using raw timestamps to determine what tasks to run when (stored as ZSET scores) 
 * does not leave any ghost tasks in Redis, using [EXPIREAT](http://redis.io/commands/expireat) to expire task keys
 * does not require a callback, task execution can be fire-and-forget and errors/info received through event subscribers


### Concurrency
Because task execution is fire-and-forget, async callbacks are not maintained by the polling mechanism and execution is non-blocking in this regard (meaning poll interval and proper task routing will never be delayed by expensive jobs). 

Task execution can potentially be blocking in the handler (TaskRunner instance), however.  This could happen if many expensive tasks of the same type are scheduled for the same time and execution of that task type requires a response callback (they can't be run in parallel for some reason).  If this occurs, Tasks could be selectively fetched & routed evenly across multiple TaskRunners running in a [node cluster](http://nodejs.org/api/cluster.html).



## Components / Exports

### Task
Base class & definition for a task or job.  [Task Model](https://github.com/DeNA/punctual/blob/master/lib/Task.js) contains task attribute getters and validations to prevent bad data from entering Redis. Task data is stored across Redis hashes and ZSET members, with a random hex task ID relating the two. The ZSET provides a queryable structure for quickly accessing a list of Task IDs, with sort maintained by scheduled execution timestamp.  

The Task class can be extended for particular use-cases requiring custom validations or data attributes.  However, it is recommended to keep task objects as lightweight as possible and subclassing the Task class would be a better alternative to extension if Task types vary greatly.  


### TaskScheduler
The purpose of the [TaskScheduler](https://github.com/DeNA/punctual/blob/master/lib/TaskScheduler.js) is to maintain a polling interval for the queue and to map scheduled tasks plucked from the queue to any number of TaskRunners.  A custom polling interval can be passed in during object instantiation, as can any flavor of logger ([log4js](https://github.com/nomiddlename/log4js-node), [bunyan](https://github.com/trentm/node-bunyan), [winston](https://github.com/flatiron/winston), etc).           

The TaskScheduler does not query Redis directly, but instead delegates this responsibility to a data-access model (TaskDAO).  This way, an alternative DAO can be used for different back-end storage in place (for instance if Task data is stored in MongoDB instead of Redis).

```javascript
// Example implementation of TaskScheduler instance
var scheduler = TaskScheduler.create({
    logger: global.logger
}).start();
```

Or if you wish to create your own TaskScheduler prototype instead:

```javascript
// Example implementation of TaskScheduler prototype
function MyCustomScheduler(opts){
    opts = opts || {};

    opts.logger = global.logger;

    this.taskRunners = opts.taskRunners || {
        exampleTaskRunner: ExampleTaskRunner.create(opts)
    };

    MyCustomScheduler.super_.call(this, opts);
}

util.inherits(MyCustomScheduler, TaskScheduler); 
```


The TaskScheduler also has the responsibility of interpreting any messages published over its subscribed channel via Redis Pub-Sub, and emitting these messages to appropriate TaskRunners.  This allows a user, process, or other remote server connected to Redis control over the system without disrupting the queue processing.  It's up to the TaskScheduler's implementation class and TaskRunner's event listeners to define what operations can be controlled via subscription.

#### Configuration Options
TaskScheduler accepts an options object with the following attributes:

```javascript
{
    pollIntervalLength: 60000,              // Frequency of the poll (in milliseconds), defaults to 30 seconds
    logger: global.logger,                  // Custom logging function, defaults to console, pass false for no logging
    redis: {                                // Redis host/port/auth, defaults to 'localhost'/6379/null 
        host: 'localhost',
        port: 6379,
        auth: 'myRedisPassword'
    },
    redisKeys: {                            // Custom Redis keys to use for persistence (defaults below)
        scheduledJobsZset: 'scheduler:tasks',     
        scheduledJobsHash: function(jobId){ 
            return 'scheduler:tasks:'+jobId.toString(); 
        }
    },
    redisTaskHashExpireAt : (60 * 60 * 24 * 3),         // Expiration for post-processed task data in seconds (prevents ghost tasks), defaults to 3 days
    redisPubSubEnabled: true,                           // Enable/disable Redis pub-sub communication, defaults to true
    redisPubSubChannel: 'scheduler',                    // Channel for Redis pub-sub communication, defaults to 'scheduler'
    taskRunners: {                                      // Task runner (handler) prototypes (can be registered later)
        myCustomTaskRunner: TaskRunner.create({ taskTypes: ['myCustomTaskType'])             
    },
    Task: CustomTaskModel                               // A custom Task model for validation (must implement a 'create' method)
}
```

#### Events
TaskScheduler emits the following events:

    
    'started' - scheduler has been initialized and has started polling
    'stopped' - scheduler has been forcibly stopped
    'poll' - emitted every time the scheduler polls Redis for tasks
    'task:failed' - a task has failed to run
    'task:completed' - a task has completed
    'task:updated' - a task has been updated
    'task:unhandled' - an unhandled task has been received
    'error' - scheduler has encountered an error



### TaskDAO
Like any other DAO or message broker, the [TaskDAO](https://github.com/DeNA/punctual/blob/master/lib/TaskDAO.js) provides an abstract interface for read/write to persisted storage (Redis in our case).  The TaskDAO module can be required independent of the other modules by other server components or remote processes (ex/ an admin or build/CI server) for easily creating tasks or collecting statistics on the queue.  The TaskDAO can be accessed through a TaskScheduler instance via: `schedulerInstance.taskDAO`.

#### Methods


    TaskDAO.getAllTasks( {function} callback )
    TaskDAO.getTaskById( {String} id, {function} callback )
    TaskDAO.getTasksInIndexRange( {Integer} lowerLimit, {Integer} upperLimit, {function} callback )
    TaskDAO.getTasksInScheduleRange( {Integer} lowerLimit, {Integer} upperLimit, {function} callback )
    TaskDAO.saveTask( {Object} task, {function} [callback] )
    TaskDAO.saveTasksMulti( {Array} tasks, {function} [callback] )
    TaskDAO.updateTaskById( {String} id, {Object} task, {function} [callback] )
    TaskDAO.removeAllTasks( {function} [callback] )
    TaskDAO.removeTasksMulti( {Array} taskIds, {function} [callback] )
    TaskDAO.removeTaskById( {String} id, {function} [callback] )
    TaskDAO.removeTaskMembersInRange( {Integer} lowerLimit, {Integer} upperLimit, {function} [callback] )





### TaskRunner
[TaskRunners](https://github.com/DeNA/redis/blob/master/lib/TaskRunner.js) are custom handlers for processing Tasks of a certain type.  As any Task could potentially require multiple levels of processing (ex/ distributing gifts to players individually), an internal queue is maintained in-memory per runner.  This processing does not (and should not) require any callbacks, so the caller (TaskScheduler) can fire-and-forget and continue performing its queue polling & mapping independently.  Note: this is in contrast to [kue](https://github.com/LearnBoost/kue) and handler functions baked into many other task queueing mechanisms.

Because it is a basis for customization, the TaskRunner base class is rather bare.  It's really up to a TaskRunner subclass implementation to define how to process tasks of types it manages.  Every subclass *must* implement a `run` method, as this is what's called by the TaskScheduler (with the task object as a parameter).  

Task runners can be assigned with scheduler initialization:
```javascript
var scriptTaskRunner = TaskRunner.create({ taskTypes: ['script', 'executable'] });

TaskRunner.prototype.runTask = function(task){
    require('child_process').exec( task.script, console.log );
};

var scheduler = TaskScheduler.create({ taskRunners: { scriptTaskRunner: scriptTaskRunner }});
```

Or they can be registered to an existing scheduler instance: 

```javascript
scheduler.registerTaskRunner( scriptTaskRunner, ['script', 'executable'] );
```




## Example Usage

See [examples/server.js](https://github.com/DeNA/redis/blob/master/examples/server.js) and [examples/worker.js](https://github.com/DeNA/redis/blob/master/examples/worker.js) for complete working usage examples.

```
cd examples
./server.js
```




## Running Tests
Tests are written in [Mocha](https://github.com/visionmedia/mocha), and can be run in development NODE_ENV.
Install dev dependencies and run tests with: 

```
NODE_ENV=development npm install
npm test 
```


HTML test coverage report can be generated via:

```
make coverage
```


## Contributing
Fork + pull-request.  Log any issues via [issue-tracker](https://github.com/DeNA/punctual/issues).




## LICENSE 

The MIT License (MIT)

Copyright (c) 2014 ngmoco, LLC.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
