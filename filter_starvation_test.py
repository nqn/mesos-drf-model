#!/usr/bin/env python

from resource_vector import ResourceVector
from mesos_allocator import Allocator, Simulator, Task

class InactiveScheduler:
    def __init__(self, name, allocator):
        self.name = name
        self.allocator = allocator

    def offer(self, offers):
        if len(offers) == 0:
            print('No offers in callback')
            return None

        print("Framework %s declined offer" % self.name)
        self.allocator.decline(self.name, offers[0])

    def status_update(self, status):
        print(status)

    def tick(self):
        pass

class StarvedScheduler:
    def __init__(self, name, allocator, job_limit):
        self.name = name
        self.allocator = allocator
        self.job_limit = job_limit
        self.task_count = 0

    def offer(self, offers):
        if len(offers) == 0:
            print('No offers in callback')
            return None

        print('offer: ' + str(offers[0].resources))
        print('launching on ' + str(self.job_limit))

        # Generate task id. Should be unique for framework.
        self.task_count += 1

        task = Task(self.name, str(self.task_count), self.job_limit)

        self.allocator.launch(task, offers[0])
        self.allocator.status()

    def status_update(self, status):
        print(status)

    def tick(self):
        pass

def main():
    allocator = Allocator()

    # Take
    allocator.add_agent('default', ResourceVector([9, 18]))

    allocator.add_framework(InactiveScheduler('A', allocator))
    allocator.add_framework(InactiveScheduler('B', allocator))
    allocator.add_framework(InactiveScheduler('C', allocator))
    allocator.add_framework(InactiveScheduler('D', allocator))
    allocator.add_framework(InactiveScheduler('E', allocator))
    allocator.add_framework(
        StarvedScheduler('F', allocator, ResourceVector([1, 3])))

    # TODO: Replace with simulator run
    simulator = Simulator(allocator)
    simulator.tick(15)


if __name__ == '__main__':
    main()


