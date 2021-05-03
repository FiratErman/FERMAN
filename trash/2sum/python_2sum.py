# %%

        
 # %%
nums = [3,2,4]
target = 6
seen={}
for index,numbers in enumerate(nums):
    remaining=target-numbers
    if remaining in seen:
        print('yes',seen[remaining],index)
    else:
        seen[numbers]=index
print(seen)
# %%
# %%
