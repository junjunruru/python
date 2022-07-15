
import pymysql
from flask import send_file
from flask_apispec import MethodResource, marshal_with, doc, use_kwargs
from . import project_swagger

def db_init():
    db = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='root',
        port=3306,
        db='city'
    )
    cursor = db.cursor(pymysql.cursors.DictCursor)
    return db, cursor

def digital_conversion(number): #將輸入數字轉為中文
    chinese_number = {
        1: '一',
        2: '二',
        3: '三',
        4: '四',
        5: '五',
        6: '六',
        7: '七',
        8: '八',
        9: '九',
        10: '十'
    }
    if len(number) == 1:
        return chinese_number[int(number)]
    else :
        if int(number[0]) > 1:
            return chinese_number[int(number[0])] + '十' + chinese_number[int(number[1])]
        else :
            return '十' + chinese_number[int(number[1])]

class python_test(MethodResource):
    @doc(description='Update User info.', tags=['政府實登']) # 文檔描述
    @use_kwargs(project_swagger.test , location='query')
    def get(self , **kwargs):
        db, cursor = db_init()
        data = {
            '市': kwargs.get('市'), 
            '區': kwargs.get('區'), 
            '總樓層數': kwargs.get('總樓層數'),
            '建物型態' : kwargs.get('建物型態')
        }
        city = data.get('市')
        area = data['區']
        total_floor = data['總樓層數']
        building_typed = data['建物型態']
        revise = [city, area, total_floor, building_typed]
        
        try:
            total_floor = total_floor.split(',')
            for i in range(len(total_floor)):
                total_floor[i] = digital_conversion(total_floor[i])
            total_floor = ','.join(total_floor)
            print(total_floor)
        except:
            pass
        try :
            city = city.replace(',','%"   or   土地位置建物門牌 LIKE "')
        except:
            pass
        try :
            area = area.replace(',','%"   or   鄉鎮市區 LIKE "')
        except:
            pass
        try :
            total_floor = total_floor.replace(',','%"   or   總樓層數 LIKE "')
        except:
            pass
        try :
            building_typed = building_typed.replace(',','%"   or   建物型態 LIKE "')
        except:
            pass
        s_list = []
        if city :
            s_list.append(f'(土地位置建物門牌 like "{city}%")')
        if area:
            s_list.append(f'(鄉鎮市區 like "{area}%")')
        if total_floor:
            s_list.append(f'(總樓層數 like "{total_floor}%")')
        if building_typed:
            s_list.append(f'(建物型態 like "%{building_typed}%")')

        if len(s_list) == 0:
            sql = '''
                select * from city.city
                '''
        else:
            sql = "select * from city.city"
            for i in range(len(s_list)):
                if i == 0:
                    sql = sql + " WHERE " + s_list[i]
                else:
                    sql = sql + " AND " + s_list[i]

        sql = sql + "limit 50 ;"
        # print('--------------------------------------------')
        # print(sql)
        # print('--------------------------------------------')

        result = cursor.execute(sql)
        if result == 0:
            db.close()
            return 'No data'
        product = cursor.fetchall()
        db.close()
        return product 
    


