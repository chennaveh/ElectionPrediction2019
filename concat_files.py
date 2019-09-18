
input_files = ['raw_clean_data/Orly_levy2019-04-06-15-16-06.csv_clean.csv']
out_file = 'clean_labels/gesher'

with open(out_file, 'a', encoding="utf8") as outfile:
    for file in input_files:
        with open(file, 'r', encoding="utf8") as infile:
            for line in infile:
                outfile.write(line)