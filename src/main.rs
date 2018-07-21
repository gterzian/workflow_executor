use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::thread;


type WorkflowId = u64;
type NumberOfSteps = usize;
type StepIndex = usize;

enum ExecutorMsg {
    Execute(Workflow),
    Quit
}

enum WorkflowMsg {
    Done(WorkflowId),
    StepExecuted(WorkflowId, StepIndex),
}

enum MainMsg {
    Done(WorkflowId),
    StepExecuted(WorkflowId, StepIndex),
}

struct Workflow {
    pub id: WorkflowId,
    pub number_of_steps: NumberOfSteps,
}

impl Workflow {
    fn new(id: WorkflowId, steps: NumberOfSteps) -> Workflow {
        Workflow {
            id: id,
            number_of_steps: steps
        }
    }
}

struct WorkflowExecution {
    pub current_step: Cell<StepIndex>,
}

struct WorkflowExecutor {
    executions: VecDeque<(Workflow, WorkflowExecution)>,
    port: Receiver<ExecutorMsg>,
    chan: Sender<WorkflowMsg>
}

impl WorkflowExecutor {
    fn handle_a_msg(&mut self) -> bool {
        match self.port.try_recv() {
            Ok(ExecutorMsg::Execute(workflow)) => {
                let execution = WorkflowExecution {
                    current_step: Cell::new(0),
                };
                self.executions.push_back((workflow, execution));
            },
            Ok(ExecutorMsg::Quit) => {
                return false
            },
            Err(_) => {
                // Empty mailbox.
            }
        }
        true
    }

    fn execute_a_step(&mut self) {
        if let Some((workflow, execution)) = self.executions.pop_front() {
            if execution.current_step.get() < workflow.number_of_steps {
                execution.current_step.set(execution.current_step.get() + 1);
                let _ = self.chan.send(WorkflowMsg::StepExecuted(workflow.id, execution.current_step.get()));
            }
            if execution.current_step.get() < workflow.number_of_steps {
                self.executions.push_back((workflow, execution));
            } else {
                let _ = self.chan.send(WorkflowMsg::Done(workflow.id));
            }
        }
    }

    fn run(&mut self) -> bool {
        if !self.handle_a_msg() {
            return false
        }
        self.execute_a_step();
        true
    }
}

fn start_executor(chan: Sender<WorkflowMsg>) -> Sender<ExecutorMsg> {
    let (executor_chan, executor_port) = channel();
    let _ = thread::Builder::new().spawn(move || {
        let mut executor = WorkflowExecutor {
            executions: Default::default(),
            port: executor_port,
            chan: chan,
        };
        while executor.run() {
            // Running...
        }
    });
    executor_chan
}

fn start_consumer(chan: Sender<MainMsg>) -> Sender<WorkflowMsg> {
    let (consumer_chan, consumer_port) = channel();
    let _ = thread::Builder::new().spawn(move || {
        for msg in consumer_port.iter() {
            match msg {
                WorkflowMsg::StepExecuted(workflow_id, index) => {
                    let _ = chan.send(MainMsg::StepExecuted(workflow_id, index));
                },
                WorkflowMsg::Done(workflow_id) => {
                    let _ = chan.send(MainMsg::Done(workflow_id));
                }
            }
        }
    });
    consumer_chan
}

#[test]
fn test_run_workflows() {
    let (results_sender, results_receiver) = channel();
    let consumer_sender = start_consumer(results_sender);
    let all_executors = vec![start_executor(consumer_sender.clone()),
                             start_executor(consumer_sender.clone())];
    let mut track_steps = HashMap::new();
    let number_of_workflows = 5;
    let number_of_steps = 4;
    {
        // Scoping the iterator, since the vec is still used later.
        let mut executors = all_executors.iter().cycle();
        for id in 0..number_of_workflows {
            let _ = track_steps.insert(id, 0);
            let workflow = Workflow::new(id, number_of_steps);
            if let Some(executor) = executors.next() {
                let _ = executor.send(ExecutorMsg::Execute(workflow));
            }
        }
    }
    let mut done = 0;
    for msg in results_receiver.iter() {
        match msg {
            MainMsg::StepExecuted(workflow_id, index) => {
                let last_step = *track_steps.get(&workflow_id).unwrap();
                // Check the order of the steps for a workflow.
                assert_eq!(last_step, index - 1);
                let _ = track_steps.insert(workflow_id, index);
            },
            MainMsg::Done(workflow_id) => {
                let last_step = *track_steps.get(&workflow_id).unwrap();
                // Check all steps were done.
                assert_eq!(last_step, number_of_steps);
                done = done + 1;
                if done == number_of_workflows {
                    for executor in all_executors {
                        let _ = executor.send(ExecutorMsg::Quit);
                    }
                    break;
                }
            }
        }
    }
    // Check all workflows are done.
    assert_eq!(done, number_of_workflows);
}
