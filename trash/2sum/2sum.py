
"""Given an array of integers nums and and integer target, return the indices
 of the two numbers such that they add up to target.

You may assume that each input would have exactly one solution, and you may not
 use the same element twice.

You can return the answer in any order."""

      
numsi=[3,2,4,8,3]
targetsi=6

def twoSum(num, target):
    seen = {}
    for i, v in enumerate(num):
        remaining = target - v
        if remaining in seen:
            return [seen[remaining], i]
        seen[v] = i
    print(seen)
    return [print(seen)]   
    print(seen)

if __name__ == "__main__":          
    print(twoSum(numsi, targetsi))




