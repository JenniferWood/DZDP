class MySentences:
    def __init__(self, filename):
        self._file_name = filename

    def __iter__(self):
        with open(self._file_name, 'r') as fopen:
            for line in fopen:
                yield line.split()
