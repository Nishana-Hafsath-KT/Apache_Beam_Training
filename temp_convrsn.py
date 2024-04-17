import apache_beam as beam

class FahrenheitToCelsius(beam.DoFn):
    def process(self, element):
        sensor_id, temp_fahrenheit = element.split(',')
        temp_celsius = (float(temp_fahrenheit) - 32) * 5.0/9.0
        return [(sensor_id, temp_celsius)]

class AveragePerSensor(beam.CombineFn):
    def create_accumulator(self):
        return (0.0, 0)

    def add_input(self, accumulator, input):
        sum, count = accumulator
        return sum + input, count + 1

    def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)

    def extract_output(self, accumulator):
        sum, count = accumulator
        return sum / count if count else float('NaN')

with beam.Pipeline() as pipeline:
    # Read input from input.csv
    input_data = (pipeline
                  | 'ReadFromText' >> beam.io.ReadFromText(r"C:\Users\user\OneDrive\Desktop\Beamlytics\temp convertn - Copy.csv")
                  )

    # Convert Fahrenheit to Celsius
    converted_temperatures = (input_data
                              | 'ConvertToFahrenheit' >> beam.ParDo(FahrenheitToCelsius())
                              )

    # Group by sensor ID and calculate average temperature
    average_temperatures = (converted_temperatures
                            | 'GroupByKey' >> beam.GroupByKey()
                            | 'CalculateAverage' >> beam.CombineValues(AveragePerSensor())
                            )

    # Format output and write to output.csv
    formatted_output = (average_temperatures
                        | 'FormatOutput' >> beam.Map(lambda x: f'{x[0]}, {x[1]}')
                        | 'WriteToText' >> beam.io.WriteToText('output1.csv')
                        )
