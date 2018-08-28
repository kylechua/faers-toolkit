import cmath
import math

def getPRR(a, b, c, d):
    if a == 0 or b == 0 or c == 0 or d == 0:
        return 0
    else:
        return (a/float(a+b)) / (c/float(c+d))

def getROR(a, b, c, d):
    if a == 0 or b == 0 or c == 0 or d == 0:
        return [0, 0, 0]
    else:
        ROR = (a/float(c)) / (b/float(d))
        try:
            UpperCI = math.exp( math.log(ROR) + 1.96*math.sqrt( 1/float(a) + 1/float(b) + 1/float(c) + 1/float(d) ) )
            LowerCI = math.exp( math.log(ROR) - 1.96*math.sqrt( 1/float(a) + 1/float(b) + 1/float(c) + 1/float(d) ) )
            return [ROR, LowerCI, UpperCI]
        except:
            return [ROR, False, False]