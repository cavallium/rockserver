import importlib.util
from pathlib import Path
import unittest
spec = importlib.util.spec_from_file_location("planner", Path(__file__).with_name("compact-adjacent-ssts.py"))
planner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(planner)

def file(n, size=10, busy=False, start=None, end=None):
    return dict(name=f"{n}.sst", sizeBytes=str(size), beingCompacted=busy,
                smallestKeyHex=start or f"{n*2:04x}", largestKeyHex=end or f"{n*2+1:04x}")

class PlannerTest(unittest.TestCase):
    def test_never_skips_large_or_busy_intervening_files(self):
        for middle in [file(2, 1000), file(2, busy=True)]:
            self.assertEqual([],planner.choose([file(1),middle,file(3)],100,1000,64))
    def test_bounds_and_hex_key_order(self):
        result=planner.choose([file(10),file(2),file(3),file(4)],100,20,2)
        self.assertEqual(["2.sst","3.sst"],[f["name"] for f in result])
    def test_shared_boundary_group_must_fit(self):
        files=[file(1,start="01",end="02"),file(2,start="02",end="03"),file(3,start="03",end="04")]
        self.assertEqual([],planner.choose(files,100,100,2))
        self.assertEqual(3,len(planner.choose(files,100,100,3)))
    def test_single_file_is_not_consolidation(self):
        self.assertEqual([],planner.choose([file(1)],100,100,64))

if __name__ == "__main__": unittest.main()
