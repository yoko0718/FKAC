
# coding: utf-8
import os
import sys
import pandas as pd

from glob import glob
from chardet.universaldetector import UniversalDetector


def File_import(Folder_path):

    df_list = []
    detector = UniversalDetector()
    file_list = glob(os.path.join(Folder_path, '*.csv'))

    for f in file_list:
        detector.reset()
        for line in open(f, 'rb'):
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        print(detector.result['encoding'])
        try:
            try:
                _df = pd.DataFrame()
                _df = pd.read_csv(
                    f, header=1, skipfooter=1,
                    encoding=detector.result['encoding'], engine='python')
            except:
                _df = pd.DataFrame()
                _df = pd.read_csv(
                    f, header=1, skipfooter=1, encoding='cp932',
                    engine='python')
        except:
            print(f)
            sys.exit('error: cannot open.')
        if _df.shape[1] == 1:
            try:
                _df = pd.DataFrame()
                _df = pd.read_csv(
                    f, header=0, encoding=detector.result['encoding'])
            except:
                _df = pd.DataFrame()
                _df = pd.read_csv(
                    f, header=0, encoding=detector.result['encoding'])
        df_list.append(_df)

    df_165 = pd.DataFrame()
    df_167 = pd.DataFrame()
    df_append = pd.DataFrame()

    for df in df_list:
        if df.shape[1] == 104:
            df_165 = pd.concat([df_165, df], axis=0)
        elif df.shape[1] == 101:
            df_167 = pd.concat([df_167, df], axis=0)
        elif df.shape[1] == 1:
            df_append = pd.concat([df_append, df], axis=0)

        if set(df_167.columns) != set(df.columns):
            print(df_167.columns)
            print(df.columns)

    df_165.reset_index(inplace=True)
    df_167.reset_index(inplace=True)
    print('165:{0}, 167:{1}, append:{2}'.format(df_165.shape, df_167.shape, df_append.shape))

    if df_165.shape[0] == 0 and df_167.shape[0] == 0:
        sys.exit('error: 0 csv file import.')

    return df_165, df_167, df_append


def Cleaning(df_165, df_167, df_append, unique_key):

    df_list = []

    df_167 = df_167.copy()[[unique_key, '被保険者証記号', '被保険者証番号', '身長', '体重', '性別', '保険者番号', '生年月日', 'ＢＭＩ', '腹囲', '収縮期血圧', '拡張期血圧', '中性脂肪（トリグリセリド）', 'ＨＤＬコレステロール', 'ＬＤＬコレステロール', 'GOT（ＡＳＴ）', 'GPT（ＡＬＴ）', 'γ-GT(γ-GTP)', '空腹時血糖（電位差法）', 'ＨｂＡ１ｃ（NGSP値）', 'ＨｂＡ１ｃ（JDS値）', '尿糖', '尿蛋白',
                            '保健指導レベル', '健診実施年月日', '服薬１（血圧）', '服薬２（血糖）', '服薬３（脂質）', '既往歴１（脳血管）', '既往歴２（心血管）', '既往歴３（腎不全・人工透析）', '貧血', '喫煙', '２０歳からの体重変化', '３０分以上の運動習慣', '歩行又は身体活動', '歩行速度', '1年間の体重変化', '食べ方1（早食い等）', '食べ方２（就寝前）', '食べ方３（夜食/間食）', '食習慣', '飲酒', '飲酒量', '睡眠', '生活習慣の改善', '保健指導の希望']]
    df_167['year'] = pd.DataFrame([
        int(str(x)[:4]) - 1 if int(str(x)[4:6]) < 4 else
        int(str(x)[:4]) for x in df_167['健診実施年月日']])

    try:
        df_165 = df_165[[unique_key, '保健指導実施年月日', '年度']]
        df_165.rename(columns={'年度': 'year'}, inplace=True)

        df_165.loc[:, '保健指導利用有無'] = [
            2 if x is None else 1 for x in df_165['保健指導実施年月日']]
        df_165 = df_165.drop('保健指導実施年月日', axis=1)
        df_165 = df_165.drop_duplicates(unique_key)

    except:
        df_165[unique_key] = df_167[unique_key]
        df_165['year'] = df_167['year']
        df_165['保健指導利用有無'] = ''
        del df_165['index']

    df = pd.merge(df_165, df_167, on=[unique_key, 'year'], how='right')

    df.columns = [
        '個人番号', 'year', '保健指導利用有無', '被保険者証記号', '被保険者証番号', '身長', '体重', '性別',
        '保険者番号', '生年月日', 'ＢＭＩ', '腹囲', '血圧(収縮期)優先', '血圧(拡張期)優先', '中性脂肪',
        'ＨＤＬコレステロール', 'ＬＤＬコレステロール', 'AST(GOT)', 'ALT(GPT）', 'γ-GT(γ-GTP)',
        '空腹時血糖', 'ＨｂＡ１ｃ（NGSP値）', 'ＨｂＡ１ｃ（JDS値）', '尿糖', '尿蛋白', '保健指導レベル', '受診日',
        '問診1', '問診2', '問診3', '問診4', '問診5', '問診6', '問診7', '問診8', '問診9', '問診10',
        '問診11', '問診12', '問診13', '問診14', '問診15', '問診16', '問診17', '問診18', '問診19',
        '問診20', '問診21', '問診22']
    df['国保取得日'] = ''
    df['国保喪失日'] = ''
    df['クレアチニン'] = ''
    df['個別/集団'] = ''

    if df_append.shape[0] != 0:
        df = df.sort_values(by=['year'], ascending=True)
        id_list = list(df[unique_key]) + list(df_append[unique_key])
        df = pd.concat([df, df.sample(n=df_append.shape[0], replace=True)])
        df[unique_key] = id_list
        df = df.drop_duplicates(subset=[unique_key], keep='first')

    return df


def Output(df, LGcode, LGname):
    if '{0:0>6}'.format(LGcode)[:2] == '{0:0>6}'.format(df['保険者番号'].unique()[0])[:2]:
        pass
    else:
        sys.exit('自治体名と自治体コードを確認してください')

    for year in sorted(df['year'].astype(int).unique()):
        df_ = df.ix[df['year'] == year]
        print(year, df_.shape)
        df_[['個人番号', 'year', '生年月日', '性別', '保険者番号','被保険者証記号', '被保険者証番号', '国保取得日','国保喪失日',
         '受診日','個別/集団','身長', '体重','ＢＭＩ', '腹囲', '血圧(収縮期)優先', '血圧(拡張期)優先', '中性脂肪',
         'ＨＤＬコレステロール', 'ＬＤＬコレステロール', 'AST(GOT)', 'ALT(GPT）', 'γ-GT(γ-GTP)',
         '空腹時血糖', 'ＨｂＡ１ｃ（JDS値）', 'ＨｂＡ１ｃ（NGSP値）', 'クレアチニン', '尿糖', '尿蛋白',
         '問診1', '問診2', '問診3', '問診4', '問診5', '問診6', '問診7', '問診8', '問診9', '問診10',
         '問診11', '問診12', '問診13', '問診14', '問診15', '問診16', '問診17', '問診18', '問診19',
         '問診20', '問診21', '問診22', '保健指導レベル', '保健指導利用有無']].drop('year', axis=1).to_csv(
            './{0}_{1}_{2}_特定健診受診歴.csv'.format(LGcode, year, LGname), encoding='cp932', index=False)

    return


if __name__ == '__main__':
    if len(sys.argv) == 5:
        _, fp, LGcode, LGname, unique_key = sys.argv
    else:
        sys.exit('python FKAC.py data_dir LGcode LGname unique_key ')
    df_165, df_167, df_append = File_import(fp)
    df = Cleaning(df_165, df_167, df_append, unique_key)
    Output(df, LGcode, LGname)
    print('done.')
