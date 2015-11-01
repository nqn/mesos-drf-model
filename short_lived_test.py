#!/usr/bin/env python

from resource_vector import ResourceVector
from mesos_allocator import Allocator, Simulator, Task


class ShortLivedScheduler:
    def __init__(self, name, allocator, job_limit):
        self.name = name
        self.allocator = allocator
        self.job_limit = job_limit
        self.task_count = 0

    def offer(self, offers):
        if len(offers) == 0:
            print('No offers in callback')
            return None

        # Generate task id. Should be unique for framework.
        self.task_count += 1

        task = Task(self.name, str(self.task_count), self.job_limit, 3)

        print('offer: ' + str(offers[0].resources))
        print('launching on ' + str(self.job_limit))

        self.allocator.launch(task, offers[0])
        self.allocator.status()

    def status_update(self, status):
        print('Status update for task "' + status.task_name + '": ' + status.status)

    def tick(self):
        print("tick")


def main():
    allocator = Allocator()

    # Take
    allocator.add_agent('default', ResourceVector([9, 18]))

    allocator.add_framework(
        ShortLivedScheduler('A', allocator, ResourceVector([1, 3])))
    allocator.add_framework(
        ShortLivedScheduler('B', allocator, ResourceVector([1, 3])))

    # TODO: Replace with simulator run
    simulator = Simulator(allocator)
    simulator.tick(15)


if __name__ == '__main__':
    main()


