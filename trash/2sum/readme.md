
def twoSum(num, target):
    seen = {}
    for i, v in enumerate(num):
        remaining = target - v
        if remaining in seen:
            return [seen[remaining], i]
        seen[v] = i
    return []   


si remaning dans seen alors:
retourn seen[r],i 
sinon seen[v]=i 


seenu={}
seenu[5]=2
seenu
output : {5: 2}

