# encoding:utf-8

old_vals = input('输入旧的水电表读数(水表，电总，电1，电2，电3)：')
new_vals = input('输入新的水电表读数(水表，电总，电1，电2，电3)：')


old_vals = [float(i) for i in old_vals.split(',')]
new_vals = [float(i) for i in new_vals.split(',')]




water_common_avg = (new_vals[0]-old_vals[0])*5/3

electric_total = new_vals[1] - old_vals[1]

one_self = new_vals[2] - old_vals[2]
two_self = new_vals[3] - old_vals[3]
three_self = new_vals[4] - old_vals[4]

electric_common_avg = (electric_total - one_self - two_self - three_self)/3

print('water_common_avg:%s' % water_common_avg)
print('electric_common_avg:%s' % electric_common_avg)

print('one_total = water_common_avg + electric_common_avg + one_self \n        = %s + %s + %s = %s\n\n' % (water_common_avg,electric_common_avg,one_self,water_common_avg+electric_common_avg+one_self))
print('two_total = water_common_avg + electric_common_avg + two_self \n        = %s + %s + %s = %s\n\n' % (water_common_avg,electric_common_avg,two_self,water_common_avg+electric_common_avg+two_self))
print('three_total = water_common_avg + electric_common_avg + three_self \n         = %s + %s + %s = %s\n\n' % (water_common_avg,electric_common_avg,three_self,water_common_avg+electric_common_avg+three_self))
