from nbformat import read
from IPython.core.getipython import get_ipython
from IPython.core.magic import Magics, magics_class, line_magic
                                
@magics_class
class RunCell(Magics):

    @line_magic
    def run_cell(self, line):
        args = line.split()
        nb = read(args[0], as_version=4)
        get_ipython().ex(nb.cells[int(args[1])].source)

def load_ipython_extension(ipython):
    ipython.register_magics(RunCell)
