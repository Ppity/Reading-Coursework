from My_Mapreduce import My_MapReduce


class wordCount_num_of_flight_taken(My_MapReduce):
    def __init__(self, input_dir, output_dir, num_mappers, num_reducers):
        My_MapReduce.__init__(self, input_dir, output_dir, num_mappers, num_reducers, clean=True)

    # overwrite function mapper
    def mapper(self, key, value):
        res = []
        count = 1
        # split string -> words use lambda
        words = [s for s in value.split('\n') if s]
        for word in words:
            res.append((word.split(',')[0], count))
        return res

    # overwrite function reducer
    def reducer(self, key, values):
        wordcount = sum(value for value in values)
        return key, wordcount


if __name__ == '__main__':
    input_dir, output_dir, n_mappers, n_reducers = "input_files", "output_files", 4, 4
    word_count = wordCount_num_of_flight_taken(input_dir, output_dir, n_mappers, n_reducers)
    word_count.run()
    print("The count result is stored in:", output_dir)
