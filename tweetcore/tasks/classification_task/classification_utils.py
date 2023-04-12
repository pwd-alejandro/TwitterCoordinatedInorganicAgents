import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tensorflow as tf
from scipy.special import softmax, expit
from sklearn.metrics import roc_curve, auc, precision_recall_curve, classification_report


def get_size_grid(number_metrics: int = None):
    rows = 1
    columns = 1

    while rows * columns < number_metrics:
        columns += 2
        if rows * columns >= number_metrics:
            break
        else:
            rows += 1
    return rows, columns


def make_report(model=None,
                y_test: pd.Series = None,
                y_prob_predicted: pd.Series = None,
                y_pre_predicted: pd.Series = None,
                x_test: pd.DataFrame = None,
                threshold: float = 0.5,
                include: list = None,
                personalized: dict = None,
                scale: float = 5,
                pad: int = 2,
                **kwargs):
    for i in include:
        if i not in ['roc', 'pr', 'lift', 'class_report', 'top_features']:
            print(
                f"'{i}' is not valid. "
                f"Options are 'roc', 'pr', 'lift', 'class_report', 'top_features', please select one")
            raise

    if y_prob_predicted is not None:
        y_prob = y_prob_predicted.copy()
    elif model is not None:
        y_prob = model.predict_proba(x_test)[:, 1]
    else:
        y_prob = np.zeros(y_test.shape[0])

    if y_pre_predicted is not None:
        y_pre = y_pre_predicted.copy()
        print('--- ignoring threshold ---')
    else:
        y_pre = y_prob.copy()
        y_pre[y_pre >= threshold] = 1
        y_pre[y_pre < threshold] = 0
        print(f'--- using threshold th = {threshold} ---')

    y_true = y_test.copy()
    rows, columns = get_size_grid(len(include))
    fig = plt.figure(figsize=(columns * scale, rows * scale))

    roc_auc = None
    report = None
    pr_auc = None
    personalized_metrics = {}
    if personalized is not None:
        for metric in personalized.keys():
            personalized_metrics[metric] = personalized[metric](y_true, y_pre, **kwargs)

    for i in range(len(include)):
        if include[i] == 'roc':
            ax = plt.subplot(rows, columns, i + 1)
            fpr, tpr, thresholds = roc_curve(y_true, y_prob)
            roc_auc = auc(fpr, tpr)
            ax.plot(fpr, tpr, lw=1, alpha=0.3, label='(AUC = %0.2f)' % roc_auc)
            ax.plot([0, 1], [0, 1], linestyle='--', lw=2, color='r', label='Chance', alpha=.8)
            ax.set_xlim([-0.05, 1.05])
            ax.set_ylim([-0.05, 1.05])
            ax.set_xlabel('False Positive Rate')
            ax.set_ylabel('True Positive Rate')
            ax.set_title('ROC curve')
            ax.legend(loc='best')
        elif include[i] == 'pr':
            ax1 = plt.subplot(rows, columns, i + 1)
            precision, recall, thresholds = precision_recall_curve(y_true, y_prob)
            pr_auc = auc(recall, precision)
            ax1.plot(recall, precision, lw=1, alpha=0.3, label='(AUC = %0.2f)' % pr_auc)
            ax1.plot([0, 1], [np.mean(y_test), np.mean(y_test)], linestyle='--', lw=2, color='r', label='Chance',
                     alpha=.8)
            ax1.set_xlim([-0.05, 1.05])
            ax1.set_ylim([-0.05, 1.05])
            ax1.set_ylabel('Precision')
            ax1.set_xlabel('Recall')
            ax1.set_title('PR curve ')
            ax1.legend(loc='best')
        elif include[i] == 'lift':
            df_dict = {'actual': list(y_true), 'pred': list(y_prob)}
            df = pd.DataFrame(df_dict)
            pred_ranks = pd.qcut(df['pred'].rank(method='first'), 100, labels=False)
            pred_percentiles = df.groupby(pred_ranks).mean()
            ax2 = plt.subplot(rows, columns, i + 1)
            ax2.set_title('Lift Chart', y=1)
            ax2.plot(np.arange(.01, 1.01, .01),
                     np.array(pred_percentiles['pred']),
                     color='darkorange',
                     lw=2,
                     label='Prediction')
            ax2.plot(np.arange(.01, 1.01, .01),
                     np.array(pred_percentiles['actual']),
                     color='navy',
                     lw=2,
                     linestyle='--',
                     label='Actual')
            ax2.plot(np.arange(.01, 1.01, .01),
                     [np.mean(y_true)] * 100,
                     color='red',
                     lw=2,
                     linestyle='--',
                     label='No skill')
            ax2.set_ylabel('Target Average')
            ax2.set_xlabel('Population Percentile')
            ax2.set_xlim([-0.05, 1.05])
            ax2.set_ylim([-0.05, 1.05])
            ax2.legend(loc="best")
        elif include[i] == 'class_report':
            report = classification_report(y_test, y_pre, output_dict=True)
            acc = round(report['accuracy'], 4)
            personalized_metrics['Accuracy'] = acc
            text = [['', '', '', round(i, 2)] for i in personalized_metrics.values()]
            df_report = pd.DataFrame(data=report)[['0', '1']].T

            ax3 = plt.subplot(rows, columns, i + 1)
            ax3.table(cellText=np.round(df_report.values, 4),
                      colLabels=df_report.columns,
                      rowLabels=df_report.index.map(lambda x: 'bot' if x == '1' else 'human'),
                      colWidths=[0.2] * 4,
                      bbox=[0.175, 0.35, 0.8, 0.6],
                      edges='closed',
                      colColours=['lightblue'] * 4,
                      rowColours=['lightblue'] * 2)

            ax3.table(cellText=text,
                      rowLabels=list(personalized_metrics.keys()),
                      colWidths=[0.2] * 4,
                      bbox=[0.22, 0.16, 0.56, 0.21],
                      edges='open')
            ax3.set_axis_off()
            ax3.set_title('Classification report')

    fig.tight_layout(pad=pad)
    plt.show()
    plt.close()

    return roc_auc, pr_auc, report, personalized_metrics


def bagging_models(models: tuple,
                   test_data: tuple,
                   train_data: tuple,
                   train_labels: np.array = None,
                   thresholds: tuple = None,
                   strategy: str = None,
                   **kwargs) -> tuple:
    strategies = ['avg_positive', 'unanimous_voting', 'sum_prob_voting', 'sum_prob_softmax', 'perceptron']
    size_test_data = [i.shape[0] for i in test_data]
    n_models = len(models)

    # checks
    if strategy not in strategies:
        print(f'--- {strategy} not included in {strategies}, please select a valid one ---')
        raise
    if max(size_test_data) != min(size_test_data):
        print(
            f'--- test datasets ought to have the same number of rows ---')
        raise
    if len(models) != len(test_data):
        print('--- the number of models and datasets ought to be the same ---')
        raise
    if train_labels.shape != (train_data[0].shape[0],):
        print(
            f'--- train y labels ought to be of shape ({train_data[0].shape[0]},) but got {train_labels.shape} instead')

    if strategy == 'avg_positive':
        print(f'--- {strategy} ---')
        print(f'--- using th = 0.5 ---')
        y_prob = np.zeros(test_data[0].shape[0])
        for i in range(n_models):
            y_prob += models[i].predict_proba(test_data[i])[:, 1]
        y_prob_avg = y_prob / n_models
        y_binary = y_prob_avg.copy()
        y_binary[y_binary >= 0.5] = 1
        y_binary[y_binary < 0.5] = 0

        return y_prob_avg, y_binary

    elif strategy == 'unanimous_voting':
        print(f'--- {strategy} ---')
        print(f'--- using thresholds = {thresholds} ---')
        print('--- for predicted prob, null is returned ---')
        y_prob = ()
        for i in range(n_models):
            y_prob += (models[i].predict_proba(test_data[i])[:, 1],)

        y_binary = np.ones(y_prob[0].shape[0])
        i = 0
        while i < n_models:
            y_binary_i = np.zeros(y_prob[0].shape[0])
            y_binary_i[y_binary_i >= thresholds[i]] = 1
            y_binary = y_binary * y_binary_i
            i += 1

        return None, y_binary

    elif strategy == 'sum_prob_voting':
        print(f'--- {strategy} ---')
        print(f'--- not using thresholds ---')
        print('--- for predicted prob, null is returned ---')
        y_prob = np.zeros((test_data[0].shape[0], 2))
        for i in range(n_models):
            y_prob += models[i].predict_proba(test_data[i])

        y_binary = np.argmax(y_prob, axis=1)

        return None, y_binary

    elif strategy == 'sum_prob_softmax':
        print(f'--- {strategy} ---')
        print(f'--- for predicted binary, null is returned ---')
        y_prob = np.zeros((test_data[0].shape[0], 2))
        for i in range(n_models):
            y_prob += models[i].predict_proba(test_data[i])

        y_prob_soft_max = np.array([softmax(i) for i in y_prob])[:, 1]

        return y_prob_soft_max, None

    elif strategy == 'perceptron':
        print(f'--- {strategy} ---')
        print(f'--- for predicted binary, null is returned ---')
        perceptron = tf.keras.models.Sequential([
            tf.keras.layers.Input(shape=(n_models, 2,)),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dense(32, activation='relu'),
            tf.keras.layers.Dense(1)])
        perceptron.summary()
        perceptron.compile(optimizer='adam',
                           loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
                           metrics=[tf.keras.metrics.Accuracy(),
                                    tf.keras.metrics.Precision(),
                                    tf.keras.metrics.Recall(),
                                    tf.keras.metrics.AUC(curve='roc', from_logits=True)])
        prob_train = np.zeros((train_data[0].shape[0], n_models, 2))
        for i in range(n_models):
            prob_train[:, i, :] += models[i].predict_proba(train_data[i])

        callback = tf.keras.callbacks.EarlyStopping(monitor='loss',
                                                    patience=3)
        perceptron.fit(prob_train,
                       train_labels,
                       callbacks=[callback],
                       **kwargs)

        prob_test = np.zeros((test_data[0].shape[0], n_models, 2))
        for i in range(n_models):
            prob_test[:, i, :] += models[i].predict_proba(test_data[i])

        # y_prob_perceptron = np.array([i[0] for i in perceptron.predict(prob_test)])
        y_prob_perceptron = np.array([expit(i[0]) for i in perceptron.predict(prob_test)])

        return y_prob_perceptron, None
