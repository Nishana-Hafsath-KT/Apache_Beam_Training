

import apache_beam as beam

# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=''):
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(self.prefix+str(element))

    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))

with beam.Pipeline() as p:
  (p | beam.Create(range(1, 50))
  # beam.combiners.Top.Largest(5) to return the larger than [5] from `PCollection`.
   | beam.combiners.Top.Largest(10)
   | Output(prefix='PCollection maximum value: '))