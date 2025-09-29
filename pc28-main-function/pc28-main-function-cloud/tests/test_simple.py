"""简单的测试文件，用于验证pytest配置"""
import pytest

def test_addition():
    """测试加法"""
    assert 1 + 1 == 2

def test_string():
    """测试字符串"""
    assert "hello" + " world" == "hello world"

def test_list():
    """测试列表操作"""
    my_list = [1, 2, 3]
    my_list.append(4)
    assert len(my_list) == 4
    assert my_list[-1] == 4

@pytest.mark.unit
def test_dictionary():
    """测试字典操作"""
    my_dict = {'a': 1, 'b': 2}
    my_dict['c'] = 3
    assert 'c' in my_dict
    assert my_dict['c'] == 3

class TestSimpleClass:
    """简单的测试类"""
    
    def test_method1(self):
        """测试方法1"""
        assert True
    
    def test_method2(self):
        """测试方法2"""
        result = [i for i in range(5)]
        assert len(result) == 5
        assert result == [0, 1, 2, 3, 4]