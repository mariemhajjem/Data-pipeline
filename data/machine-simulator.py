import simpy
import random

speed_1 = 3  # Avg. processing time of Machine 1 in minutes
speed_2 = 4  # Processing time of Machine 2 in minutes
time = 120  # Sim time in minutes


# Class setup for Machine 1
# This needs to be done as there are varying processing speeds
class Machine(object):
    """
    A machine produces units at a fixed processing speed,
    takes units from a store before and puts units into a store after.

    Machine has a *name*
    Next steps:
    - Machine produces units at distributed processing speeds.
    - A machine fails at fixed intervals and is repaired at a fixed time.
    - Failure and repair times are distributed.

    """

    def __init__(self, env, name, in_q, out_q, speed):
        self.env = env
        self.name = name
        self.in_q = in_q
        self.out_q = out_q
        self.speed = speed

        # Start the producing process
        self.process = env.process(self.produce())

    def produce(self):
        """
        Produce parts as long as the simulation runs.

        """
        while True:
            part = yield self.in_q.get()
            print(f'{self.env.now:.2f} Machine {self.name} has got a part')

            yield env.timeout(self.speed)
            print(
                f'{self.env.now:.2f} Machine {self.name} finish a part next q has {len(self.out_q.items)} and capacit of {self.out_q.capacity}')

            yield self.out_q.put(part)
            print(f'{self.env.now:.2f} Machine {self.name} pushed part to next buffer')


def gen_arrivals(env, entry_buffer):
    """
    start the process for each part by putting
    the part in the starting buffer
    """

    while True:
        yield env.timeout(random.uniform(1, 4))
        print(f'{env.now:.2f} part has arrived')
        part = object()  # too lazy to make a real part class

        yield entry_buffer.put(part)


# Create environment and start the setup process
env = simpy.Environment()
bufferStart = simpy.Store(env)  # Buffer with unlimited capacity
buffer1 = simpy.Store(env, capacity=6)  # Buffer between machines with limited capacity
bufferEnd = simpy.Store(env)  # Last buffer with unlimited capacity

# the machines __init__ starts the machine process so no env.process() is needed here
machine_1 = Machine(env, 'Machine 1', bufferStart, buffer1, speed_1)
machine_2 = Machine(env, 'Machine 2', buffer1, bufferEnd, speed_2)
# machine_3 = Machine(env, 'Machine 3', buffer1, bufferEnd, speed_2)

env.process(gen_arrivals(env, bufferStart))

# Execute
env.run(until=time)

