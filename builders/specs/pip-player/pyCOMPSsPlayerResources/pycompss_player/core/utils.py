

def get_object_method_by_name(obj, method_name):
    for class_method_name in dir(obj):
        if callable(getattr(obj, class_method_name)):
            if class_method_name.startswith(method_name) or method_name in class_method_name:
                return class_method_name

def table_print(col_names, data):
    row_format ="{:>15}" * (len(col_names) + 1)
    print(row_format.format("", *col_names))
    for col_name, row in zip(col_names, data):
        print(row_format.format('-', *row))