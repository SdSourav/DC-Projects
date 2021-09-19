def func(n):

    # Base case (When n becomes 0 or negative)
    if (n == 0):
        return

    func(n - 1)

    # Then print increasing order
    print(n, end=" ")


# Driver Code
n = 5
func(n)
