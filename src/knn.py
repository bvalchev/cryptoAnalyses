import nltk
from nltk.corpus import wordnet as wn
from nltk.corpus import genesis
nltk.download('genesis')
nltk.download('wordnet')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
genesis_ic = wn.ic(genesis, False, 0.0)
import time

from sklearn.feature_extraction.text import TfidfVectorizer


import numpy as np
import pandas as pd
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from nltk.stem import SnowballStemmer
from nltk.stem.lancaster import LancasterStemmer

from sklearn.metrics import roc_auc_score

dataset = pd.read_csv('../data/knnInput.csv')
print(np.max(dataset['movement_the_day_after']))
print(np.mean(dataset['movement_the_day_after']))
print(np.min(dataset['movement_the_day_after']))
dataset['output'] = round(dataset['movement_the_day_after'] / 1000)
print(dataset.head(1000)['output'])

class KNN_NLC_Classifer():
    def __init__(self, k = 1, distance_type = 'path'):
        self.k = k
        self.distance_type = distance_type
        self.vectorizer = TfidfVectorizer(stop_words='english')


    # This function is used for training
    def fit(self, x_train, y_train):
        self.x_train = x_train
        self.y_train = y_train

    def document_similarity(self, doc1, doc2):
        """Finds the symmetrical similarity between doc1 and doc2"""

        synsets1 = self.doc_to_synsets(doc1)
        synsets2 = self.doc_to_synsets(doc2)

        return (self.similarity_score(synsets1, synsets2) + self.similarity_score(synsets2, synsets1)) / 2


    def cosine_sim(self, text1, text2):
        tfidf = self.vectorizer.fit_transform([text1, text2])
        return ((tfidf * tfidf.T).A)[0, 1]

    # This function runs the K(1) nearest neighbour algorithm and
    # returns the label with closest match.
    def predict(self, x_test):
        self.x_test = x_test
        y_predict = []

        tic = time.perf_counter()
        tfidf = TfidfVectorizer().fit_transform(dataset['message'][1:1000])
        pairwise_similarity = tfidf * tfidf.T
        arr = pairwise_similarity.toarray()
        np.fill_diagonal(arr, np.nan)
        result_idx = np.nanargmax(arr[998])
        print(dataset['message'][result_idx])
        toc = time.perf_counter()
        print(pairwise_similarity)
        print(f"Pairwise built in {toc - tic:0.4f} seconds")
        # no need to normalize, since Vectorizer will return normalized tf-idf
        print('we ready')


        for i in range(len(x_test)):
            tic = time.perf_counter()

            max_sim = 0
            max_index = 0
            for j in range(self.x_train.shape[0]):
                temp = self.cosine_sim(x_test[i + train_size + 1], self.x_train[j])#self.document_similarity(x_test[i + train_size + 1], self.x_train[j])
                if temp > max_sim:
                    max_sim = temp
                    max_index = j
            y_predict.append(self.y_train[max_index])
            print(self.y_train[max_index])
            print(test_corpus.loc[i + train_size + 1, 'output'])
            print(self.y_train[max_index] == test_corpus.loc[i + train_size + 1, 'output'])
            toc = time.perf_counter()
            print(f"This is how much we are fucked {toc - tic:0.4f} seconds")
        return y_predict

    def convert_tag(self, tag):
        """Convert the tag given by nltk.pos_tag to the tag used by wordnet.synsets"""
        tag_dict = {'N': 'n', 'J': 'a', 'R': 'r', 'V': 'v'}
        try:
            return tag_dict[tag[0]]
        except KeyError:
            return None

    def doc_to_synsets(self, doc):
        """
            Returns a list of synsets in document.
            Tokenizes and tags the words in the document doc.
            Then finds the first synset for each word/tag combination.
        If a synset is not found for that combination it is skipped.

        Args:
            doc: string to be converted

        Returns:
            list of synsets
        """
        tokens = word_tokenize(doc + ' ')

        l = []
        tags = nltk.pos_tag([tokens[0] + ' ']) if len(tokens) == 1 else nltk.pos_tag(tokens)

        for token, tag in zip(tokens, tags):
            syntag = self.convert_tag(tag[1])
            syns = wn.synsets(token, syntag)
            if (len(syns) > 0):
                l.append(syns[0])
        return l

    def similarity_score(self, s1, s2, distance_type='path'):
        """
        Calculate the normalized similarity score of s1 onto s2
        For each synset in s1, finds the synset in s2 with the largest similarity value.
        Sum of all of the largest similarity values and normalize this value by dividing it by the
        number of largest similarity values found.

        Args:
            s1, s2: list of synsets from doc_to_synsets

        Returns:
            normalized similarity score of s1 onto s2
        """
        s1_largest_scores = []

        for i, s1_synset in enumerate(s1, 0):
            max_score = 0
            for s2_synset in s2:
                if distance_type == 'path':
                    score = s1_synset.path_similarity(s2_synset, simulate_root=False)
                else:
                    score = s1_synset.wup_similarity(s2_synset)
                if score != None:
                    if score > max_score:
                        max_score = score

            if max_score != 0:
                s1_largest_scores.append(max_score)

        mean_score = np.mean(s1_largest_scores)

        return mean_score

train_size = int(0.995 * len(dataset))
test_size = int(0.005 * len(dataset))
print(test_size)

train_corpus = dataset[:train_size]
test_corpus = dataset[-test_size:]
X_train = train_corpus['message']

y_train = train_corpus['output']


classifier = KNN_NLC_Classifer(k=1, distance_type='path')
classifier.fit(X_train, y_train)

y_pred_final = classifier.predict(test_corpus['message'])

num_all = len(test_corpus)
num_correct = 0

for j in range(len(y_pred_final)):
    if y_pred_final[j] == test_corpus['output']:
        num_correct += 1

print(num_correct/num_all)

# output_df = pd.DataFrame(data={'text': final_test_list, 'code': y_pred_final})
# output_df['answer'] = np.where(output_df['code'] == 1, 'Temperature', 'Conditions')
# print(output_df)