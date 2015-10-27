#!/usr/bin/python

import operator

from resource_vector import ResourceVector


class Simulator:
    def __init__(self, allocator):
        self.allocator = allocator

    def tick(self, ticks=1):
        # Simulator have to drive allocations for now.
        # Allocation interval is default 1s in Mesos.
        for _ in range(ticks):
            self.allocator.allocate()


class Offer:
    def __init__(self, agent, resources):
        self.agent = agent
        self.resources = resources


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


class Agent:
    def __init__(self, name, resources):
        self.name = name
        self.drf = DRFList(resources)

    def add_framework(self, name):
        self.drf.add_user(name)


class Allocator:
    def __init__(self):
        self.agents = {}
        self.frameworks = {}
        self.filters = {}

    def add_agent(self, name, resources):
        self.agents[name] = Agent(name, resources)

    def remove_agent(self, name):
        pass

    def add_framework(self, framework):
        self.frameworks[framework.name] = framework
        for agent_name, agent in self.agents.iteritems():
            agent.add_framework(framework.name)

    def allocate(self):
        for agent_name, agent in self.agents.iteritems():
            order = agent.drf.order()

            # TODO: Filter framework if still filtered.
            if len(order) == 0:
                print("No frameworks to serve")
                return

            resources = agent.drf.available()

            # Order all available resources to first framework.
            name, _ = order[0]

            if name in self.filters:
                print '%s is filtered: continue' % name
                pass

            agent.drf.allocate(name, resources)

            # Form offer to scheduler
            offer = Offer(agent, resources)
            self.frameworks[name].offer([offer])

    def recover(self, agent_name, resources, user_name):
        self.agents[agent_name].drf.recover(user_name, resources)

    def accept(self, user_name, resources, offer):
        recover = offer.resources.subtract(resources)
        self.recover(offer.agent.name, recover, user_name)

    def decline(self, user_name, offer, refuse_steps=5):
        self.filters[user_name] = refuse_steps
        self.recover(offer.agent.name, offer.resources, user_name)

    def status(self):
        for agent_name, agent in self.agents.iteritems():
            print 'total:    ' + str(agent.drf.total)
            print 'consumed: ' + str(agent.drf.consumed)

            for user_name, user in agent.drf.users.iteritems():
                print 'consumed by ' + user_name + ' : ' + str(user)

            print 'share:    ' + str(agent.drf.order())

    def tick(self):
        pass


class DRFList:
    def __init__(self, resources):
        # Available resources
        self.total = resources

        # Consumed resource
        self.consumed = ResourceVector([0] * resources.dimensions())

        # User vector
        self.users = {}

        # Max fair share per user
        self.max_fair_share = {}

    def available(self):
        return self.total.subtract(self.consumed)

    def order(self):
        return sorted(self.max_fair_share.items(), key=operator.itemgetter(1))

    def add_user(self, name):
        self.max_fair_share[name] = 0.0
        self.users[name] = ResourceVector([0] * self.total.dimensions())

    def allocate(self, user, resources):
        # Update consumed vector
        self.consumed = self.consumed.add(resources)

        # Update user vector
        self.users[user] = self.users[user].add(resources)

        s = self.users[user].divide(self.total)
        self.max_fair_share[user] = max(s.vector)

    def recover(self, user, resources):
        # Update consumed vector
        self.consumed = self.consumed.subtract(resources)

        # Update user vector
        self.users[user] = self.users[user].subtract(resources)

        s = self.users[user].divide(self.total)
        self.max_fair_share[user] = max(s.vector)


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
