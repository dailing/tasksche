import unittest

from tasksche.functional import CachedPropertyWithInvalidator, _INVALIDATE


class T:
    def __init__(self, next=None):
        self.next = next
        self.cal_cnt = 0

    @CachedPropertyWithInvalidator
    def a(self):
        self.cal_cnt += 1
        return self.cal_cnt

    @a.register_broadcaster
    def a_broadcaster(self):
        if self.next is not None:
            return self.next
        return []


class MyTestCase(unittest.TestCase):
    def test_something(self):
        t1 = T()
        t2 = T([t1])
        t3 = T([t1])
        t4 = T([t2, t3])
        self.assertEqual(t4.a, 1)
        # test assert
        self.assertEqual(t4.a, 1)
        self.assertEqual(t2.a, 1)
        t4.a = None
        self.assertEqual(t4.a, None)
        # test broadcast
        self.assertEqual(t1.a, 1)
        self.assertEqual(t2.a, 2)
        self.assertEqual(t3.a, 1)
        t4.a = _INVALIDATE
        self.assertEqual(t4.a, 2)
        self.assertEqual(t1.a, 2)
        self.assertEqual(t2.a, 3)
        self.assertEqual(t3.a, 2)


if __name__ == '__main__':
    unittest.main()
