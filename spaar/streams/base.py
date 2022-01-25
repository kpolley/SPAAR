class Stream:
    def __init__(self, spark):
        self._spark = spark
        self._df = None
        print("initializing")
    
    def read(self):
        raise NotImplementedError

    def transform(self):
        return self._df
    
    def load(self):
        raise NotImplementedError

    def run(self):
        self.read()
        self.transform()
        self.load()

        self._df.awaitTermination()