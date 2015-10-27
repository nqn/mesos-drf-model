#!/usr/bin/python


class DRF:
    def __init__(self):
        self.R = []
        self.C = []
        self.s = []
        self.D = []

    def schedule(self):
        m = len(self.R)

        # pick user i with lowest dominant share s_i
        s_i = None
        i = None
        for cur_i in range(len(self.s)):
            if s_i is None:
                s_i = self.s[cur_i]
                i = cur_i
            else:
                if s_i > self.s[cur_i]:
                    s_i = self.s[cur_i]
                    i = cur_i

        if i is None:
            print("No users available: cannot schedule")
            return

        # Di <- demand of user i's next task
        Ds = self.D[i]
        if len(Ds) == 0:
            print("User has no jobs to schedule")
            return

        D = Ds[0]

        # Missing in paper: choose machine with available capacity
        j = None
        for r in range(m):
            # Per resouce dimension
            for k in range(len(self.R[r])):
                if D[k] > self.R[r][k]:
                    j = None
                    break
                j = r

            if j is not None:
                break

        for k in range(len(D)):
            # Update consumed vector
            self.C[j][k] = self.C[j][k] + D[k]

            # Update user vector
            self.U[i][k] = self.U[i][k] + D[k]

        # si = max^m_j=1{u_{i,j}/r_j}
        s_max = None
        for j in range(m):
            for k in range(len(D)):
                s = float(self.U[i][k]) / self.R[j][k]

                if s_max is None:
                    s_max = s
                else:
                    s_max = max(s_max, s)

        self.s[i] = s_max

        print "schedule: %d" % i
        print 'C: ' + str(self.C)
        print 'U: ' + str(self.U)
        print 's: ' + str(self.s)


def main():
    drf = DRF()

    # Add slaves
    drf.R = [[9, 18]]
    drf.C = [[0, 0]]

    # Add users
    drf.s = [0, 0]
    drf.U = [[0, 0], [0, 0]]

    # Add jobs
    jobs_a = [[1, 4], [1, 4], [1, 4]]
    jobs_b = [[3, 1], [3, 1], [3, 1]]
    drf.D = [jobs_b, jobs_a]

    drf.schedule()
    drf.schedule()
    drf.schedule()
    drf.schedule()
    drf.schedule()


if __name__ == '__main__':
    main()
