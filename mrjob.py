from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, _, line):
        line = line.strip()
        words = line.split()
        for word in words:
            yield (word.lower(), 1)

    def reducer (self, word, count):
        yield None, (word, sum(count))

    def finalizer(self, result):
        sorted_result = sorted(result, reverse=True)[:3]
        yield sorted_result

if __name__ == "__main__":
    WordCount.run()