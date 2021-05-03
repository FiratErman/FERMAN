"""
Given a sorted array nums, remove the duplicates in-place such 
that each element appear only once and return the new length.
"""

nums = [1,1,2,5,5,6,6,7,7]
seen=[]
def tw(nums,seen):
    for i in nums:
        if i not in seen:
            seen.append(i)
    return len(seen)



if __name__== "__main__":
    print(tw(nums,seen))