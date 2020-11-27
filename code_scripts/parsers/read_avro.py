import pandas
import fastavro


def read_avro_df(filepath, encoding):
    with open(filepath, encoding) as fp:
        reader = fastavro.reader(fp)
        records = [r for r in reader]
        df = pandas.DataFrame.from_records(records)
        return df