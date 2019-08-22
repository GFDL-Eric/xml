import numpy as np
from datetime import timedelta

def atm_factors(n):
    return list(set((i, n//i) for i in range(2, int(n**0.5) + 1) if n % i == 0 and i < n and ((n//(i**2) < 5 and n//(i**2) > 1) or i == 3)))

def ice_factors(n):
    return list(set((i, n//i) for i in range(2, int(n**0.5) + 1) if i == 3 ))

def determine_io(in_factor):
    if in_factor % 4 == 0:
        return 4
    elif in_factor % 3 == 0:
        return 3
    elif in_factor % 2 == 0:
        return 2
    else:
        return 1

def blanks(incoming_integer):
    return ' ' * incoming_integer

class Cluster():
    def __init__(self, parent, cluster):
        self.parent = parent
        self.cluster = cluster
        if "c3" in self.cluster:
            self.cores_per_node = 32
        elif "c4" in self.cluster:
            self.cores_per_node = 36
        else:
            raise NameError

        self.ranks = self.cores_per_node * self.parent.nodes
        self.layout_cores = self.ranks / 6
        self.possible_atm_layouts = atm_factors(self.layout_cores)
        self.ice_layout = ice_factors(self.ranks)[0]

    def make_io_layouts(self):
        self.atm_io_layout = (1, determine_io(self.atm_layout[1]))
        self.ice_io_layout = (1, determine_io(self.ice_layout[0]))

    def make_layout_strings(self):
        self.atm_layout_string = '{},{}'.format(str(self.atm_layout[0]), str(self.atm_layout[1]))
        self.ice_layout_string = '{},{}'.format(str(self.ice_layout[1]), str(self.ice_layout[0]))
        self.atm_io_layout_string = '{},{}'.format(str(self.atm_io_layout[0]), str(self.atm_io_layout[1]))
        self.ice_io_layout_string = '{},{}'.format(str(self.ice_io_layout[0]), str(self.ice_io_layout[1]))

    def resolve_layouts(self):
        self.make_io_layouts()
        self.make_layout_strings()

    def write_freinclude(self):
        atm_ranks_padding = 4 - len(str(self.ranks))
        ocn_ranks_padding = 3
        blank_ranks_padding = 12
        blank_threads_padding = 11
        atm_layout_padding = 8 - len(self.atm_layout_string)
        ice_layout_padding = 8 - len(self.ice_layout_string)
        atm_io_layout_padding = 5 - len(self.atm_io_layout_string)
        ice_io_layout_padding = 5 - len(self.ice_io_layout_string)
        if 'on' in self.parent.ht:
            ht_padding = 1
        else:
            ht_padding = 0
        blank_ht_padding = 17
        string_lines = []
        string_lines.append('{}<site="{}">'.format(blanks(4), self.cluster))
        string_lines.append('{}<atm ranks="{}"{} threads="{}" layout="{}"{} io_layout="{}"{} hyperthread="{}"{} />'.format(blanks(6), str(self.ranks), blanks(atm_ranks_padding), str(self.parent.threads), self.atm_layout_string, blanks(atm_layout_padding), self.atm_io_layout_string, blanks(atm_io_layout_padding), self.parent.ht, blanks(ht_padding)))
        string_lines.append('{}<lnd {} {} layout="{}"{} io_layout="{}"{} {} />'.format(blanks(6), blanks(blank_ranks_padding), blanks(blank_threads_padding), self.atm_layout_string, blanks(atm_layout_padding), self.atm_io_layout_string, blanks(atm_io_layout_padding), blanks(blank_ht_padding)))
        string_lines.append('{}<ocn ranks="0"{} threads="0" layout="{}"{} io_layout="{}"{} hyperthread="off" />'.format(blanks(6), blanks(ocn_ranks_padding), self.ice_layout_string, blanks(ice_layout_padding), self.ice_io_layout_string, blanks(ice_io_layout_padding), blanks(blank_ht_padding)))
        string_lines.append('{}<ice {} {} layout="{}"{} io_layout="{}"{} {} />'.format(blanks(6), blanks(blank_ranks_padding), blanks(blank_threads_padding), self.ice_layout_string, blanks(ice_layout_padding), self.ice_io_layout_string, blanks(ice_io_layout_padding), blanks(blank_ht_padding)))
        string_lines.append('{}<site/>\n'.format(blanks(4)))
        return '\n'.join(string_lines)

class ResourceTag():
    def __init__(self, nodes=3, ht='off', omp='off'):
        self.nodes = nodes
        self.threads = 1
        self.ht = ht
        if 'on' in self.ht:
            self.threads *= 2
        self.omp = omp
        if 'on' in self.omp:
            self.threads *= 2
        self.name = '{}nodes_ht_{}_omp_{}'.format(str(self.nodes), self.ht, self.omp)
        self.freinclude_block = ''
        self.wallclock = self.clock_to_str()
        self.c3 = Cluster(self, "c3")
        self.c4 = Cluster(self, "c4")
        self.matched_atm_layouts = self.match_layouts() 

    def clock_to_str(self):
        clock_td = timedelta(minutes=540 // self.nodes)
        hours = clock_td.seconds//3600
        minutes = (clock_td.seconds//60) % 60
        if minutes < 3 and hours == 0:
            minutes = 3
        return '{}:{}:{}'.format(str(hours).zfill(2), str(minutes).zfill(2), str(0).zfill(2))

    def match_layouts(self):
        layout_diff = 99999
        c3idx = 0
        c4idx = 0
        for c3i, c3l in enumerate(self.c3.possible_atm_layouts):
            for c4i, c4l in enumerate(self.c4.possible_atm_layouts):
                this_diff = np.abs(c3l[0] - c4l[0]) + np.abs(c3l[1] - c4l[1])
                if this_diff < layout_diff:
                    layout_diff = this_diff
                    c3idx = c3i
                    c4idx = c4i
        self.c3.atm_layout = self.c3.possible_atm_layouts[c3idx]
        self.c3.alt_atm_layouts = list(set(self.c3.possible_atm_layouts) - set(self.c3.atm_layout))
        self.c4.atm_layout = self.c4.possible_atm_layouts[c4idx]
        self.c4.alt_atm_layouts = list(set(self.c4.possible_atm_layouts) - set(self.c4.atm_layout))

    def resolve_cluster_layouts(self):
        self.c3.resolve_layouts()
        self.c4.resolve_layouts()

    def write_prefix(self):
        string_lines = []
        string_lines.append('<freInclude name="{}">'.format(self.name))
        string_lines.append('  <resources jobWallclock="{}">\n'.format(self.wallclock))
        self.freinclude_block += '\n'.join(string_lines)
        return

    def write_suffix(self):
        string_lines = []
        string_lines.append('  </resources>')
        string_lines.append('</freInclude>')
        string_lines.append('\n')
        self.freinclude_block += '\n'.join(string_lines)
        return

    def write_all(self):
        self.write_prefix()
        self.freinclude_block += self.c3.write_freinclude()
        self.freinclude_block += self.c4.write_freinclude()
        self.write_suffix()

    def resolve_and_write(self):
        self.resolve_cluster_layouts()
        self.write_all()

def write_full(node_count_list=[3,6,9,12,24,48,96,192], ht_options=['off', 'on'], omp_options=['off', 'on'], outfile='test_file.xml'):
    for nd_count in node_count_list:
        for ht_opt in ht_options:
            for omp_opt in omp_options:
                my_rt = ResourceTag(nodes=nd_count, ht=ht_opt, omp=omp_opt)
                my_rt.resolve_and_write()
                with open(outfile, 'a+') as fh:
                    fh.write(my_rt.freinclude_block)

if __name__ == '__main__':
    write_full()

