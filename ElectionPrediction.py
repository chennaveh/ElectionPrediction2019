import os
import patoolib
import operator
import datetime
import csv
import random
import time
import threading
from collections import Counter


def delete_output_files():
    for f in os.listdir('output/'):
        os.unlink(os.path.join('output/', f))


def remove_if_exist(input_file):
    if os.path.exists(input_file):
        os.remove(input_file)


def create_rar(rar_filename, input_file):
    remove_if_exist(rar_filename)
    patoolib.create_archive(rar_filename, (input_file,), verbosity=-1)


def shuffle_data(input_file, output_file, limit_lines=False, n_lines=0):
    with open(input_file, 'r', encoding="utf8") as source:
        data = [(random.random(), line) for line in source]
    data.sort()
    with open(output_file, 'w', encoding="utf8") as target:
        for i, (_, line) in enumerate(data):
            if limit_lines and i > n_lines - 1:
                break
            target.write(line)


class ElectionPrediction(object):

    def __init__(self, party_files, labels):
        self.party_files = party_files
        self.labels = labels
        self.squeezed_class = {}
        self.squeezed_test = {}
        self.results = {}
        self.tbp = {}
        self.tbp_lock = threading.Lock()
        self.result_lock = threading.Lock()
        for label, file in zip(labels, party_files):
            rar_filename = 'output/' + label + '_cls.rar'

            create_rar(rar_filename, file)
            self.squeezed_class[label] = os.path.getsize(rar_filename)

    def squeeze(self, test_file, n_threads):
        if n_threads == 0:
            raise ValueError('trying to use threading when n_threads = 0')

        # prepare files for threads
        lines_per_thread = int(sum(1 for l in open(test_file, encoding="utf8")) / n_threads)
        # print(lines_per_thread)
        with open(test_file, 'r', newline='', encoding="utf8") as test:
            lines = test.readlines()
            for i in range(n_threads):
                with open('output/test_{}.csv'.format(i), 'w', encoding="utf8", newline='') as outfile:
                    outfile.writelines(lines[i*lines_per_thread:((i+1)*lines_per_thread)])

        thread_list = []
        for i in range(n_threads):
            t = threading.Thread(target=self.squeezer, args=('output/test_{}.csv'.format(i), i))
            t.start()
            thread_list.append(t)

        for t in thread_list:
            t.join()

        tbp = {}
        for i in self.tbp.values():
            for p, tweet in i.items():
                if p not in tbp:
                    tbp[p] = []
                tbp[p].append(tweet)
        self.tbp = tbp

        sum_results = Counter()
        total_votes = 0
        for thread_result in self.results.values():
            for party, votes in thread_result.items():
                sum_results[party] += votes
                total_votes += votes
        sum_results = dict(reversed(sorted(sum_results.items(), key=operator.itemgetter(1))))
        return {party: votes/total_votes*120 for party, votes in sum_results.items()}

    def squeezer(self, test_file, idx):
        results = Counter()
        # num_lines = sum(1 for l in open(test_file, encoding="utf8"))
        tweets_by_party = {}
        with open(test_file, newline='', encoding="utf8") as test:
            reader = csv.reader(test)
            i = 0
            for tweet in reader:
                # print('line {} / {}'.format(i, num_lines))
                i += 1
                # print(tweet)
                if len(tweet) == 0:
                    continue
                entropy = {}
                for label, party in zip(self.labels, self.party_files):
                    now = datetime.datetime.now()
                    now_str = now.strftime("%Y-%m-%d-%H-%M-%S-%f")
                    out_file = 'output/' + str(idx) + label + '_' + now_str + '_test.txt'
                    rar_file = 'output/' + str(idx) + label + '_' + now_str + '_test.rar'
                    remove_if_exist(out_file)
                    with open(party, 'r', encoding="utf8") as p:
                        with open(out_file, 'w', encoding="utf8") as outfile:
                            outfile.writelines(p)
                            outfile.write(tweet[0])
                        create_rar(rar_file, out_file)
                    entropy[label] = os.path.getsize(rar_file) - self.squeezed_class[label]
                min_entropy_label = min(entropy.items(), key=operator.itemgetter(1))[0]
                results[min_entropy_label] += 1
                if min_entropy_label not in tweets_by_party:
                    tweets_by_party[min_entropy_label] = []
                tweets_by_party[min_entropy_label].append(tweet[0])
                if i % 200 == 0:
                    for f in os.listdir('output/'):
                        if f.startswith(str(idx)):
                            os.unlink(os.path.join('output/', f))
        self.result_lock.acquire()
        self.results[idx] = results
        print('save results thread {}'.format(idx))
        self.result_lock.release()
        self.tbp_lock.acquire()
        self.tbp[idx] = tweets_by_party
        self.tbp_lock.release()


labels = ['gesher',
          'haavoda',
          'hadash',
          # 'ichud_meflagot_hayamin',
          'israel_beyteno',
          'kachol_lavan',
          'likud',
          'meretz',
          'shas',
          'yamin_chadash',
          'zehut']
train_files = ['data/' + party + '.csv' for party in labels]
test_file = 'data/test.csv'
test_file_shuffle = 'data/test_shuffle.csv'

delete_output_files()
shuffle_data(test_file, test_file_shuffle, limit_lines=True, n_lines=5000)


start = time.time()
predictor = ElectionPrediction(train_files, labels)
n_threads = 10
votes = predictor.squeeze(test_file_shuffle, n_threads)
total = time.time() - start

print('total time {}:{}'.format(int(total / 60), int(total % 60)))
print("\n".join("{}\t{:.2f}".format(p, v) for p, v in votes.items()))

delete_output_files()

for party in predictor.tbp:
    fname = "output/" + 'tbp_' + party + ".txt"
    with open(fname, 'w', encoding="utf8") as f:
        f.writelines('\n'.join(line) for line in predictor.tbp[party])

