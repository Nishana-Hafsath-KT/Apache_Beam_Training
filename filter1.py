import apache_beam as beam

def is_strawberry(plant):
    return plant['icon'] == '🍓'

with beam.Pipeline() as p:
    strawberries = (
          p | 'Gardening plants' >> beam.Create([
          {
              'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'
          },
          {
              'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'
          },
          {
              'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'
          },
          {
              'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'
          },
          {
              'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'
          },
      ])
        | 'Filter strawberries' >> beam.Filter(is_strawberry)
        | beam.Map(print)
    )



import apache_beam as beam

with beam.Pipeline() as p:
    strawberries = (
        p | 'Gardening plants' >> beam.Create([
            {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
            {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
            {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
            {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
            {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
        ])
        | 'Filter strawberries' >> beam.Filter(lambda plant: plant['icon'] == '🍓')
        | beam.Map(print)
    )
