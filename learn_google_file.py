import fire

class Calculator(object):
  """A simple calculator class."""


  def double(self, number,n2):
    return 2 * number*n2

if __name__ == '__main__':
    c = Calculator()
    fire.Fire(c.double)