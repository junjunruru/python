from marshmallow import fields ,Schema


class test(Schema): # 定義接收的資料
    市 = fields.Str(example="臺北市,新北市,桃園市,臺中市,高雄市") 
    區 = fields.Str(example="板橋區,新莊區,中和區,桃園區,臺中區")
    總樓層數 = fields.Str(example="阿拉伯數字") 
    建物型態 = fields.Str(example="大樓,電梯,其他")
