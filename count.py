import apache_beam as beam

with beam.Pipeline() as p:
  total_elements_per_keys = (
      p | 'Create plants' >> beam.Create([
          ('spring', '🍓'),
          ('spring', '🥕'),
          ('summer', '🥕'),
          ('fall', '🥕'),
          ('spring', '🍆'),
          ('winter', '🍆'),
          ('spring', '🍅'),
          ('summer', '🍅'),
          ('fall', '🍅'),
          ('summer', '🌽'),])
      | 'Count elements per key' >> beam.combiners.Count.PerKey()
      | beam.Map(print))
  


