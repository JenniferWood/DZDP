from db import mongo


class FileSentences:
    def __init__(self, filename):
        self._file_name = filename

    def __iter__(self):
        with open(self._file_name, 'r') as fopen:
            for line in fopen:
                yield line.split()


class DbDataProcess:
    _dao = mongo.MyMongoDb('dzdp')

    def __init__(self):
        pass

    @staticmethod
    def get_review_overall_score(star, score_list):
        score = 0
        num = 0

        if star is not None:
            score = float(star)
            num += 1

        if score_list is not None and len(score_list) > 0:
            for concrete_rate in score_list:
                if concrete_rate < 0:
                    continue
                score += concrete_rate
                num += 1

        if num > 0:
            score /= num

        return score
