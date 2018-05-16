#coding: utf-8
import csv
import os
from datetime import datetime as dt
from collections import Counter


def __fild_all_files__(directory):
    for root, dirs, files in os.walk(directory):
        yield root
        for file in files:
            yield os.path.join(root, file)


def not_required_index(header_list):
    """
    headerリストを読み込んで、不必要な列のインデックスを返す関数
    """

    # 必要なデータ
    required = [
        '保険者番号',
        '広域連合番号',
        '被保険者証記号',
        '被保険者証番号',
        '生年月日元号',
        '生年月日',
        '性別',
        '個人番号',
        'データ管理番号１',
        '受診券整理番号',
        '健診実施年月日',
        '健診機関コード',
        '身長',
        '体重',
        'ＢＭＩ',
        '内臓脂肪面積',
        '腹囲',
        '既往歴',
        '既往歴',
        '自覚症状',
        '自覚症状',
        '他覚症状',
        '他覚症状',
        '収縮期血圧',
        '拡張期血圧',
        '採血時間（食後）',
        '中性脂肪（トリグリセリド）',
        'ＨＤＬコレステロール',
        'ＬＤＬコレステロール',
        'GOT（ＡＳＴ）',
        'GPT（ＡＬＴ）',
        'γ-GT(γ-GTP)',
        '空腹時血糖（電位差法）',
        '随時血糖（電位差法）',
        'ＨｂＡ１ｃ（NGSP値）',
        'ＨｂＡ１ｃ（JDS値）',
        '尿糖',
        '尿蛋白',
        'ヘマトクリット値',
        '血色素量',
        '赤血球数',
        '貧血検査',
        '心電図',
        '心電図',
        '心電図',
        '眼底検査',
        '眼底検査',
        '眼底検査',
        'メタボリックシンドローム判定',
        '保健指導レベル',
        '医師の診断（判定）',
        '健康診断を実施した医師の氏名',
        '服薬１（血圧）',
        '服薬確認者（血圧）',
        '服薬２（血糖）',
        '服薬確認者（血糖）',
        '服薬３（脂質）',
        '服薬確認者（脂質）',
        '既往歴１（脳血管）',
        '既往歴２（心血管）',
        '既往歴３（腎不全・人工透析）',
        '貧血',
        '喫煙',
        '２０歳からの体重変化',
        '３０分以上の運動習慣',
        '歩行又は身体活動',
        '歩行速度',
        '1年間の体重変化',
        '食べ方1（早食い等）',
        '食べ方２（就寝前）',
        '食べ方３（夜食/間食）',
        '食習慣',
        '飲酒',
        '飲酒量',
        '睡眠',
        '生活習慣の改善',
        '保健指導の希望',
        '健診実施保険者',
        '受診券整理番号',
        '利用券発行保険者',
        '利用券整理番号',
        'ＨＬ身長',
        'ＨＬ体重',
        'ＨＬＢＭＩ',
        'ＨＬ内臓脂肪面積',
        'ＨＬ腹囲',
        'ＨＬ収縮期血圧',
        'ＨＬ拡張期血圧',
        'ＨＬ中性脂肪（トリグリセリド）',
        'ＨＬＨＤＬコレステロール',
        'ＨＬＬＤＬコレステロール',
        'ＨＬGOT（ＡＳＴ）',
        'ＨＬGPT（ＡＬＴ）',
        'ＨＬγ-GT(γ-GTP)',
        'ＨＬ空腹時血糖（電位差法）',
        'ＨＬ随時血糖（電位差法）',
        'ＨＬＨｂＡ１ｃ（NGSP値）',
        'ＨＬＨｂＡ１ｃ（JDS値）',
        'ＨＬヘマトクリット値',
        'ＨＬ血色素量',
        'ＨＬ赤血球数'
    ]
    # 必要なデータの個数
    limit = Counter(required)

    not_required = []
    counts = Counter()
    for index, item in enumerate(header_list):
        counts[item] += 1
        if (item in limit and counts[item] > limit[item]) or item not in required:
            not_required.append(index)
    return (not_required)


if __name__ == '__main__':
    log_name = f'{dt.now():%Y%m%d_%H%M}'
    path = os.path.abspath(".")
    file_list = [
        x for x in __fild_all_files__(path)
        if ('.csv' in x.lower()) & os.path.isfile(x)]
    for files in file_list:
        if 'FKAC167' in files and '処理後' not in files:
            raw_file = files
            file_name = '【処理後】' + files.split('/')[-1]
            with open(raw_file, newline='',encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
                pre_header=next(reader)
                pre_header[0]=pre_header[0].replace('\ufeff','')
                header = next(reader)[0].split(',')
                data = []
                for row in reader:
                    data.append(row)

            not_required = not_required_index(header)

            dellist = lambda items, indexes: [item for index, item in enumerate(items) if index not in indexes]
            cleaned_data=[dellist(data[n][0].split(','),not_required) for n in range(len(data))]
            cleaned_header = dellist(header, not_required)

            with open(file_name, 'w', newline='',encoding='　cp932') as csvfile:
                writer = csv.writer(csvfile, delimiter=',',
                                    quotechar=' ', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(pre_header)
                writer.writerow(cleaned_header)
                writer.writerows(cleaned_data)