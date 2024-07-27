import sys
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC


def main():
    filename1 = sys.argv[1]
    filename2 = sys.argv[2]

    data_labelled = pd.read_csv(filename1)
    data_unlabelled = pd.read_csv(filename2)

    columns = data_labelled.drop(['city'], axis=1)
    X = columns.drop(['year'], axis=1)
    X_columns = X.to_numpy()
    y_column = data_labelled['city'].to_numpy()

    X_train, X_valid, y_train, y_valid = train_test_split(X_columns, y_column)
    model = make_pipeline(
        StandardScaler(),
        SVC(kernel='linear', C=0.1)
    )
    model.fit(X_train, y_train)
    print(model.score(X_valid, y_valid))

    columns = data_unlabelled.drop(['city'], axis=1)
    X_unlab = columns.drop(['year'], axis=1)
    X_data_unlabelled = X_unlab.to_numpy()
    predictions = model.predict(X_data_unlabelled)
    pd.Series(predictions).to_csv(sys.argv[3], index=False, header=False)


if __name__ == '__main__':
    main()