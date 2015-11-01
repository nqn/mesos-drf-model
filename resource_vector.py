class ResourceVector:
    def __init__(self, vector):
        self.vector = vector

    def represent(self):
        return self.vector

    def validate_input_vector(self, right):
        if right is None:
            print "Right value is none"
            return False

        if len(right.vector) is not len(self.vector):
            print "Resource dimensions do not match: %d != %d" % (len(right), len(self.vector))
            return False

        return True

    def add(self, right):
        if self.validate_input_vector(right) is False:
            return None

        return ResourceVector([x+y for x, y in zip(self.vector, right.vector)])

    def divide(self, right):
        if self.validate_input_vector(right) is False:
            return None

        return ResourceVector([float(x)/float(y) for x, y in zip(self.vector, right.vector)])

    def subtract(self, right):
        if self.validate_input_vector(right) is False:
            return None

        return ResourceVector([x-y for x, y in zip(self.vector, right.vector)])

    def __str__(self):
        return str(self.vector)

    def dimensions(self):
        return len(self.vector)
