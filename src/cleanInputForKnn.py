import pandas as pd
import re
import nltk

from nltk.corpus import stopwords

dataset = pd.read_csv('../data/mergedData.csv')


nltk.download('stopwords')
s = stopwords.words('english')
ps = nltk.wordnet.WordNetLemmatizer()

for i in range(len(dataset)):
    message = re.sub('[^a-zA-Z]', ' ', dataset.loc[i,'message'])
    message = message.lower()
    message = message.split()
    message = [ps.lemmatize(word) for word in message if not word in s]
    message = ' '.join(message)
    dataset.loc[i, 'message'] = message

dataset.to_csv('../data/knnInput.csv')