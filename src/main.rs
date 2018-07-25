#![feature(mpsc_select)]

use std::cell::Cell;
use std::collections::{HashMap, VecDeque};
use std::sync::mpsc::{Select, Sender, Receiver, channel};
use std::thread;


type WorkflowId = usize;
type NumberOfSteps = usize;
type StepIndex = usize;
type ExecutorId = usize;

enum ExecutorMsg {
    Execute(Workflow),
    Quit
}

enum ExecutorControlMsg {
    WantsWork,
    DoesNotWantWork
}

enum ConsumerMsg {
    Done(WorkflowId),
    StepExecuted(WorkflowId, StepIndex),
    ExpectWorkflow(WorkflowId),
}

#[derive(Debug, PartialEq)]
enum ConsumerControlMsg {
    AllWorkflowDone
}

enum ProducerMsg {
    Incoming(Workflow),
}

enum ProducerControlMsg {
    Quit
}

enum MainMsg {
    FromProducer(ProducerMsg),
    FromConsumer(ConsumerControlMsg),
    FromExecutor((ExecutorId, ExecutorControlMsg))
}

#[derive(Clone)]
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


struct ExecutorControlChan {
    id: ExecutorId,
    chan: Sender<(ExecutorId, ExecutorControlMsg)>
}

impl ExecutorControlChan {
    fn send(&self, msg: ExecutorControlMsg) {
        let _ = self.chan.send((self.id, msg));
    }
}


struct WorkflowExecution {
    pub current_step: Cell<StepIndex>,
}


struct WorkflowExecutor {
    executions: VecDeque<(Workflow, WorkflowExecution)>,
    port: Receiver<ExecutorMsg>,
    chan: Sender<ConsumerMsg>,
    control_chan: ExecutorControlChan
}

impl WorkflowExecutor {
    fn handle_a_msg(&mut self) -> bool {
        match self.port.try_recv() {
            Ok(ExecutorMsg::Execute(workflow)) => {
                let execution = WorkflowExecution {
                    current_step: Cell::new(0),
                };
                self.executions.push_back((workflow, execution));
                if self.executions.len() > 2 {
                    self.control_chan.send(ExecutorControlMsg::DoesNotWantWork);
                }
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
                let _ = self.chan.send(ConsumerMsg::StepExecuted(workflow.id, execution.current_step.get()));
            }
            if execution.current_step.get() < workflow.number_of_steps {
                self.executions.push_back((workflow, execution));
            } else {
                let _ = self.chan.send(ConsumerMsg::Done(workflow.id));
                if self.executions.len() < 2 {
                    self.control_chan.send(ExecutorControlMsg::WantsWork);
                }
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

struct WorkflowProducer {
    port: Receiver<ProducerControlMsg>,
    chan: Sender<ProducerMsg>,
    number_of_workflows: usize,
    produced_workflows: Cell<usize>,
    number_of_steps: NumberOfSteps,
}

impl WorkflowProducer {
    fn receive_a_message(&self) -> bool {
        match self.port.try_recv() {
            Ok(ProducerControlMsg::Quit) => {
                return false
            },
            Err(_) => {
                // Empty mailbox.
            }
        }
        true
    }

    fn produce_a_workflow(&self) {
        if self.produced_workflows.get() < self.number_of_workflows {
            let workflow = Workflow::new(self.produced_workflows.get(), self.number_of_steps);
            let _ = self.chan.send(ProducerMsg::Incoming(workflow));
            self.produced_workflows.set(self.produced_workflows.get() + 1);
        }
    }

    fn run(&mut self) -> bool {
        if !self.receive_a_message() {
            return false
        }
        self.produce_a_workflow();
        true
    }
}

fn start_executor(chan: Sender<ConsumerMsg>, control_chan: ExecutorControlChan) -> Sender<ExecutorMsg> {
    let (executor_chan, executor_port) = channel();
    let _ = thread::Builder::new().spawn(move || {
        let mut executor = WorkflowExecutor {
            executions: Default::default(),
            port: executor_port,
            chan: chan,
            control_chan: control_chan
        };
        while executor.run() {
            // Running...
        }
    });
    executor_chan
}

fn start_consumer(chan: Sender<ConsumerControlMsg>,
                  number_of_workflows: usize,
                  number_of_steps: usize)
                  -> Sender<ConsumerMsg> {
    let (consumer_chan, consumer_port) = channel();
    let _ = thread::Builder::new().spawn(move || {
        let mut track_steps = HashMap::new();
        let mut done = 0;
        for msg in consumer_port.iter() {
            match msg {
                ConsumerMsg::StepExecuted(workflow_id, index) => {
                    let last_step = *track_steps.get(&workflow_id).unwrap();
                    // Check the order of the steps for a workflow.
                    assert_eq!(last_step, index - 1);
                    let _ = track_steps.insert(workflow_id, index);
                },
                ConsumerMsg::Done(workflow_id) => {
                    let last_step = *track_steps.get(&workflow_id).unwrap();
                    // Check all steps were done.
                    assert_eq!(last_step, number_of_steps);
                    done = done + 1;
                    if done == number_of_workflows {
                        let _ = chan.send(ConsumerControlMsg::AllWorkflowDone);
                    }
                }
                ConsumerMsg::ExpectWorkflow(workflow_id) => {
                    let _ = track_steps.insert(workflow_id, 0);
                }
            }
        }
    });
    consumer_chan
}

fn start_producer(chan: Sender<ProducerMsg>,
                  number_of_workflows: usize,
                  number_of_steps: usize)
                  -> Sender<ProducerControlMsg> {
    let (producer_chan, producer_port) = channel();
    let _ = thread::Builder::new().spawn(move || {
        let mut producer = WorkflowProducer {
            port: producer_port,
            chan: chan,
            number_of_workflows: number_of_workflows,
            number_of_steps: number_of_steps,
            produced_workflows: Default::default()
        };
        while producer.run() {
            // Running...
        }
    });
    producer_chan
}

fn executor_control_chan(id: ExecutorId,
                         chan: Sender<(ExecutorId, ExecutorControlMsg)>)
                         -> ExecutorControlChan {
    ExecutorControlChan {
        id: id,
        chan: chan
    }
}

#[test]
fn test_run_workflows() {
    let (results_sender, results_receiver) = channel();
    let (work_sender, work_receiver) = channel();
    let (executor_control_sender, executor_control_receiver) = channel();
    let number_of_workflows = 5;
    let number_of_steps = 4;
    let consumer_sender = start_consumer(results_sender, number_of_workflows, number_of_steps);
    let producer_chan = start_producer(work_sender, number_of_workflows, number_of_steps);
    let executors = vec![(start_executor(consumer_sender.clone(),
                                         executor_control_chan(1, executor_control_sender.clone())),
                          1,
                          Cell::new(true)),
                          (start_executor(consumer_sender.clone(),
                                         executor_control_chan(2, executor_control_sender.clone())),
                          2,
                          Cell::new(true))];
    let mut executor_queue: VecDeque<(Sender<ExecutorMsg>, ExecutorId, Cell<bool>)> = executors.into_iter().collect();
    loop {
        let msg = {
            let sel = Select::new();
            let mut work_port = sel.handle(&work_receiver);
            let mut results_port = sel.handle(&results_receiver);
            let mut executor_port = sel.handle(&executor_control_receiver);
            unsafe {
                work_port.add();
                results_port.add();
                executor_port.add();
            }
            let ready = sel.wait();
            if ready == work_port.id() {
                MainMsg::FromProducer(work_port.recv().unwrap())
            } else if ready == results_port.id() {
                MainMsg::FromConsumer(results_port.recv().unwrap())
            } else if ready == executor_port.id() {
                MainMsg::FromExecutor(executor_port.recv().unwrap())
            } else {
                panic!("unexpected select result")
            }
        };
        let result = match msg {
             MainMsg::FromProducer(ProducerMsg::Incoming(workflow)) => {
                let _ = consumer_sender.send(ConsumerMsg::ExpectWorkflow(workflow.id));
                let mut handled = false;
                while !handled {
                    let (sender, id, active) = executor_queue.pop_front().unwrap();
                    if active.get() {
                        let _ = sender.send(ExecutorMsg::Execute(workflow.clone()));
                        handled = true;
                    }
                    executor_queue.push_back((sender, id, active));
                }
                continue;
            },
            MainMsg::FromExecutor((executor_id, msg)) => {
                match msg {
                    ExecutorControlMsg::WantsWork => {
                        for (_sender, id, active) in executor_queue.iter_mut() {
                            if *id == executor_id {
                                active.set(true);
                                break;
                            }
                        }
                    }
                    ExecutorControlMsg::DoesNotWantWork => {
                        for (_sender, id, active) in executor_queue.iter_mut() {
                            if *id == executor_id {
                                active.set(false);
                                break;
                            }
                        }
                    }
                }
                continue;
            },
            MainMsg::FromConsumer(msg) => msg
        };
        // The only message the consumer will send, is that all is done.
        assert_eq!(result, ConsumerControlMsg::AllWorkflowDone);
        for (executor, _, _) in executor_queue {
            let _ = executor.send(ExecutorMsg::Quit);
        }
        let _ = producer_chan.send(ProducerControlMsg::Quit);
        break;
    }
}
