"""示例测试文件"""

import pytest

def test_addition():
    """测试加法"""
    assert 2 + 2 == 4

def test_string_operations():
    """测试字符串操作"""
    assert "hello" + " " + "world" == "hello world"
    assert "python".upper() == "PYTHON"

@pytest.mark.unit
def test_list_operations():
    """测试列表操作"""
    test_list = [1, 2, 3]
    test_list.append(
