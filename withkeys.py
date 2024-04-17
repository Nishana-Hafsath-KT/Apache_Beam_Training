


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
  (p | beam.Create(['apple', 'banana', 'cherry', 'durian', 'guava', 'melon'])
   # The WithKeys return Map which key will be first letter word and value word
   | beam.WithKeys(lambda word: word[0:1])
   | Output(prefix='PCollection with-keys value: '))