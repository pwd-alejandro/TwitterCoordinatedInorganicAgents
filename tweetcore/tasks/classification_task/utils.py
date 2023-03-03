import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
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


def make_report(model,
                y_test: pd.Series,
                x_test: pd.DataFrame,
                include: list = None,
                scale: float = 5,
                pad: int = 2):
    for i in include:
        if i not in ['roc', 'pr', 'lift', 'class_report', 'top_features']:
            print(
                f"'{i}' is not valid. "
                f"Options are 'roc', 'pr', 'lift', 'class_report', 'top_features', please select one")
            raise
    y_true = y_test.copy()
    y_pre = model.predict(x_test)
    number_metrics = len(include)
    rows, columns = get_size_grid(number_metrics)
    fig = plt.figure(figsize=(columns * scale, rows * scale))
    for i in range(len(include)):
        if include[i] == 'roc':
            ax = plt.subplot(rows, columns, i + 1)
            fpr, tpr, thresholds = roc_curve(y_true, y_pre)
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
            precision, recall, thresholds = precision_recall_curve(y_true, y_pre)
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
        elif include[i] == 'class_report':
            report = classification_report(y_test, y_pre, output_dict=True)
            acc = round(report['accuracy'], 4)
            df_report = pd.DataFrame(data=report)[['0', '1']].T

            ax2 = plt.subplot(rows, columns, i + 1)
            ax2.table(cellText=np.round(df_report.values, 4),
                      colLabels=df_report.columns,
                      rowLabels=df_report.index.map(lambda x: 'bot' if x == '1' else 'human'),
                      colWidths=[0.2] * 4,
                      bbox=[0.175, 0.35, 0.8, 0.6],
                      edges='closed',
                      colColours=['lightblue'] * 4,
                      rowColours=['lightblue'] * 2)

            ax2.table(cellText=[['', '', acc]],
                      rowLabels=['Accuracy'],
                      colWidths=[0.2] * 3,
                      bbox=[0.21, 0.17, 0.55, 0.2],
                      edges='open')
            ax2.set_axis_off()
            ax2.set_title('Classification report')

    fig.tight_layout(pad=pad)
    plt.show()
