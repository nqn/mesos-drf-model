#!/usr/bin/python

from collections import OrderedDict

from event_bus import EventSource, initialize_event_bus, publish_event

import json

import operator

from resource_vector import ResourceVector


global_time = 0
initialize_event_bus(lambda: global_time)


class Simulator(EventSource):
    def __init__(self, allocator):
        self.allocator = allocator

    def source_name(self):
        return "simulator"

    def source_id(self):
        return "mesos_drf_simulator"

    def tick(self, ticks=1):
        global global_time

        # Simulator have to drive allocations for now.
        # Allocation interval is default 1s in Mesos.
        for _ in range(ticks):
            for agent in self.allocator.agents.itervalues():
                agent.tick()

            for framework in self.allocator.frameworks.itervalues():
                framework.tick()

            self.allocator.tick()
            global_time += 1


class Task:
    def __init__(self, framework_name, task_name, resources, duration=None):
        self.framework_name = framework_name
        self.task_name = task_name
        self.resources = resources
        self.duration = duration


class Offer:
    def __init__(self, agent, resources):
        self.agent = agent
        self.resources = resources


class Agent(EventSource):
    def __init__(self, name, resources, allocator):
        self.name = name
        self.drf = DRFList(resources)
        self.filters = {}
        self.frameworks = {}
        self.allocator = allocator

    def source_name(self):
        return "agent"

    def source_id(self):
        return self.name

    def add_framework(self, name):
        publish_event(self, "add_framework", name)
        self.drf.add_user(name)

    def add_filter(self, framework_name, duration):
        event_data = { "framework_name": framework_name, "duration": duration }
        publish_event(self, "add_filter", event_data)
        print 'Adding filter with duration %s for framework %s' % (duration,
                                                                   framework_name)
        self.filters[framework_name] = duration

    def get_filter(self, framework_name):
        return self.filters.get(framework_name)

    def launch(self, task):
        publish_event(self, "launch_task", task)
        if task.framework_name not in self.frameworks:
            self.frameworks[task.framework_name] = {}

        if task.task_name in self.frameworks[task.framework_name]:
            print("WARNING: Overriding task %s on agent %s" % (task.task_name, self.name))

        self.frameworks[task.framework_name][task.task_name] = task

    def tick(self):
        for framework_name in self.filters.keys():
            new_duration = self.filters[framework_name] - 1
            self.filters[framework_name] = new_duration
            if new_duration <= 0:
                event_data = { "framework_name": framework_name }
                publish_event(self, "clear_filter", event_data)
                print 'Clearing filter for framework %s' % framework_name
                del self.filters[framework_name]

        for framework_name in self.frameworks.keys():
            for task_name in self.frameworks[framework_name].keys():
                if self.frameworks[framework_name][task_name].duration is None:
                    # Task never terminates.
                    continue

                new_duration = self.frameworks[framework_name][task_name].duration - 1
                self.frameworks[framework_name][task_name].duration = new_duration

                if new_duration <= 0:
                    print('Task %s completed' % task_name)
                    del self.frameworks[framework_name][task_name]

                    update = Update(self.name, framework_name, task_name, "FINISHED")

                    self.allocator.status_update(update)


class Update:
    def __init__(self, agent_name, framework_name, task_name, status):
        self.agent_name = agent_name
        self.framework_name = framework_name
        self.task_name = task_name
        self.status = status


class Allocator:
    def __init__(self):
        self.agents = {}
        self.frameworks = OrderedDict()
        self.tasks = {}

    def source_name(self):
        return "allocator"

    def source_id(self):
        return "mesos_drf_allocator"

    def add_agent(self, name, resources):
        event_data = { "agent_name": name, "resources": resources }
        publish_event(self, "add_agent", event_data)
        self.agents[name] = Agent(name, resources, self)

    def remove_agent(self, name):
        event_data = { "agent_name": name }
        publish_event(self, "remove_agent", event_data)
        pass

    def add_framework(self, framework):
        publish_event(self, "add_framework", framework.name)
        self.frameworks[framework.name] = framework
        for agent_name, agent in self.agents.iteritems():
            agent.add_framework(framework.name)

    def allocate(self):
        for agent_name, agent in self.agents.iteritems():
            order = agent.drf.order()

            if len(order) == 0:
                print("No frameworks to serve")
                return

            resources = agent.drf.available()

            # Ensure we haven't exhausted resources on the agent.
            out_of_capacity = False
            for resource in resources.vector:
                if resource <= 0:
                    print("Agent %s is out of capacity" % agent_name)
                    out_of_capacity = True
            if out_of_capacity:
                continue

            # Offer all available resources to first framework.
            for framework_name, _ in order:
                print 'Allocator considering framework %s' % framework_name
                if agent.get_filter(framework_name):
                    print 'Framework %s is filtered: continue' % framework_name
                    continue

                agent.drf.allocate(framework_name, resources)

                # Form offer to scheduler
                offer = Offer(agent, resources)

                event_data = {
                    "agent_name": agent.name,
                    "framework_name": framework_name,
                    "resources": resources,
                }
                publish_event(self, "resource_offer", event_data)

                self.frameworks[framework_name].offer([offer])
                break

    def recover(self, agent_name, resources, user_name):
        event_data = {
            "agent_name": agent_name,
            "resources": resources,
            "user_name": user_name,
        }
        publish_event(self, "recover_resources", event_data)
        self.agents[agent_name].drf.recover(user_name, resources)

    def launch(self, task, offer):
        for i in range(len(task.resources.vector)):
            if task.resources.vector[i] > offer.resources.vector[i]:
                print("Framework tried to launch larger task than offer resources")
                return

        recover = offer.resources.subtract(task.resources)
        self.recover(offer.agent.name, recover, task.framework_name)

        # Schedule task on agent
        self.agents[offer.agent.name].launch(task)

        # Save reference to task to keep track of resources to free up.
        if task.framework_name not in self.tasks:
            self.tasks[task.framework_name] = {}

        publish_event(self, "launch_task", task)

        self.tasks[task.framework_name][task.task_name] = task

    def decline(self, framework_name, offer, refuse_steps=5):
        event_data = {
            "framework_name": framework_name,
            "agent_name": offer.agent.name,
            "resources": offer.resources,
            "filter_duration": refuse_steps,
        }
        publish_event(self, "decline_offer", event_data)
        offer.agent.add_filter(framework_name, refuse_steps)
        self.recover(offer.agent.name, offer.resources, framework_name)

    def status(self):
        for agent_name, agent in self.agents.iteritems():
            print 'total:    ' + str(agent.drf.total)
            print 'consumed: ' + str(agent.drf.consumed)

            for user_name, user in agent.drf.users.iteritems():
                print 'consumed by ' + user_name + ' : ' + str(user)

            print 'share:    ' + str(agent.drf.order())

    def tick(self):
        self.allocate()

    def status_update(self, update):
        task = self.tasks[update.framework_name][update.task_name]

        # Recover resources if terminal. For now, those are the only type of statuses; so recover for now.
        self.recover(update.agent_name, task.resources, update.framework_name)

        # Inform framework about task status.
        self.frameworks[update.framework_name].status_update(update)

        # Don't keep track of task any longer as it just completed.
        del self.tasks[update.framework_name][update.task_name]


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
