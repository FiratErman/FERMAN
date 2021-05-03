import bid 

#create a function to find for each player his highest bid
def bid_winner (Bid):
    highestbid={}
    winner=[]
    for i in bid.Buyers:
        highestbid[i]=max(bid.Buyers[i])
    return highestbid

#Winner is the one with the highest bid
winner=max(bid_winner(bid.Buyers))

#Select the second highest bid as the winning price
list_highest_bid=list(bid_winner(bid.Buyers).values())
list_highest_bid.sort()
winning_price=list_highest_bid[-2]


if __name__ == "__main__":
    print('winner is', winner)
    print('winning price is', winning_price)
    