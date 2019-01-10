import sys
import time

class TextProgressBar(object):
    def __init__(self, width=50, style="*."):
        self.width = 50
        self.style = style

    def show(self, percent):
        a = self.style[0] * int(self.width * percent)
        b = self.style[1] * (self.width - int(self.width * percent))
        sys.stdout.write("\r[{0}{1}] {2:.0f}%% completed.".format(a, b, percent*100))


if __name__ == '__main__':
    bar = TextProgressBar()
    for i in range(99):
        bar.show(i/99)
        time.sleep(0.2)
    print()