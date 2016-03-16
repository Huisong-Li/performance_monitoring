1.  执行sql_script文件夹中的sql脚本
2   ./Lib/site-packages/xlrd/formula.py
    func_defs字典中键为184和186中间添加
    186行左右，在文字184和文字189中间加插入一行
    186: ('HACKED', 1, 1, 0x02, 1, 'V', 'V'),
3.  估值表放入source文件夹中
4.  使用Python 2.7.9 执行
4.1 在performance_monitoring文件夹所在目录下 执行 python -W ignore -m performance_monitoring.py
5.  执行后的结果在destination文件夹中