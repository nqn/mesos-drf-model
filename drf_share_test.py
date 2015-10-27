#!/usr/bin/env python

from resource_vector import ResourceVector
from mesos_allocator import Allocator, Simulator

class Scheduler:
    def __init__(self, name, allocator, job_limit):
        self.name = name
        self.allocator = allocator
        self.job_limit = job_limit

    def offer(self, offers):
        if len(offers) == 0:
            print('No offers in callback')
            return None

        print('offer: ' + str(offers[0].resources))
        print('launching on ' + str(self.job_limit))

        self.allocator.accept(self.name, self.job_limit, offers[0])
        self.allocator.status()


def main():
    allocator = Allocator()

    # Take
    allocator.add_agent('default', ResourceVector([9, 18]))

    allocator.add_framework(Scheduler('B', allocator, ResourceVector([1, 4])))
    allocator.add_framework(Scheduler('A', allocator, ResourceVector([3, 1])))

    # TODO: Replace with simulator run
    simulator = Simulator(allocator)
    simulator.tick(5)


if __name__ == '__main__':
    main()

