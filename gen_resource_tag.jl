abstract type Layout end
struct AtmLayout <: Layout
    layout::Tuple{Int,Int}
    AtmLayout(layout) = new(layout)
end

struct IceLayout <: Layout
    layout::Tuple{Int,Int}
    IceLayout(layout) = new(layout)
end

struct ResourceTagSetup
    nodes::Int
    ht::String
    omp::String
    ResourceTagSetup(nodes, ht, omp) = new(nodes, ht, omp)
end

struct ResourceTag
    setup::ResourceTagSetup
    threads::Int
    name::String
    wallclock::String
    prefix::String
    suffix::String
    ResourceTag(setup, threads, name, wallclock, prefix, suffix) = new(setup, threads, name, wallclock, prefix, suffix)
end

struct Cluster
    ρ::ResourceTag
    κ::String
    cores_per_node::Int
    ranks::Int
    layout_cores::Int
    possible_atm_layouts::Array{AtmLayout}
    ice_layout::IceLayout
    Cluster(ρ, κ, cores_per_node, ranks, layout_cores, possible_atm_layouts, ice_layout) = new(ρ, κ, cores_per_node, ranks, layout_cores, possible_atm_layouts, ice_layout)
end

determine_io(α::AtmLayout) = mod(α.layout[2],4) == 0 ? 4 : mod(α.layout[2],3) == 0 ? 3 : mod(α.layout[2],2) == 0 ? 2 : 1
determine_io(ι::IceLayout) = mod(ι.layout[1],4) == 0 ? 4 : mod(ι.layout[1],3) == 0 ? 3 : mo
function calc_threads(σ::ResourceTagSetup)
    ht = σ.ht == "off" ? 1 : σ.ht == "on" ? 2 : throw(DomainError(σ.ht, "argument must be \"off\" or \"on\""))
    omp = σ.omp == "off" ? 1 : σ.omp == "on" ? 2 : throw(DomainError(σ.omp, "argument must be \"off\" or \"on\""))
    ht * omp
end

atm_factors(χ::Int) = unique([round.(Int, (i,fld(χ,i))) for i=2:floor(sqrt(χ)) if mod(χ,i) == 0 && i < χ && ((fld(χ,i^2) < 5 && fld(χ,i^2) > 1) || i == 3)])
ice_factors(χ::Int) = unique([round.(Int, (i,fld(χ,i))) for i=2:floor(sqrt(χ)) if i == 3])

rt_name(σ::ResourceTagSetup) = "$(repr(σ.nodes))nodes_ht_$(σ.ht)_omp_$(σ.omp)"

function clock_to_str(ν::Int)
    seconds = fld(540 * 60, ν)
    hours = fld(seconds, 3600)
    minutes = hours == 0 ? max(3,mod(fld(seconds, 60), 60)) : mod(fld(seconds, 60), 60)
    "$(lpad(repr(hours),2,repr(0))):$(lpad(repr(minutes),2,repr(0))):00"
end

function make_cluster(ρ::ResourceTag, κ::String)
    cores_per_node = κ == "c3" ? 32 : κ == "c4" ? 36 : throw(DomainError(κ, "cluster not recognized, current clusters are \"c3\" and \"c4\"."))
    ranks = cores_per_node * ρ.setup.nodes
    layout_cores = div(ranks, 6)
    possible_atm_layouts = [AtmLayout(atm_fact) for atm_fact in atm_factors(layout_cores)]
    ice_layout = IceLayout(ice_factors(ranks)[1])
    my_cluster = Cluster(ρ, κ, cores_per_node, ranks, layout_cores, possible_atm_layouts, ice_layout)
end

make_io_layout(λ::Layout) = (1, determine_io(λ))

#make_layout_string(λ::Layout) = $Layout.layout(1, determine_io(λ))

#    def make_layout_strings(self):
#        self.atm_layout_string = f'{str(self.atm_layout[0])},{str(self.atm_layout[1])}'
#        self.ice_layout_string = f'{str(self.ice_layout[1])},{str(self.ice_layout[0])}'
#        self.atm_io_layout_string = f'{str(self.atm_io_layout[0])},{str(self.atm_io_layout[1])}'
#        self.ice_io_layout_string = f'{str(self.ice_io_layout[0])},{str(self.ice_io_layout[1])}'
#
function match_layouts(γ::Array{AtmLayout,1}, ϵ::Array{AtmLayout,1})
    layout_diff = 99999
    c3idx = 0
    c4idx = 0
    for (c3i, c3l) in enumerate(γ)
        for (c4i, c4l) in enumerate(ϵ)
            this_diff = abs(c3l.layout[1] - c4l.layout[1]) + abs(c3l.layout[2] - c4l.layout[2])
            if this_diff < layout_diff
                layout_diff = this_diff
                c3idx = c3i
                c4idx = c4i
            end
        end
    end
    α = γ[c3idx]
    α_alt = collect(delete!(Set(γ),α))
    β = ϵ[c4idx]
    β_alt = collect(delete!(Set(ϵ),β))
    return (α, α_alt, β, β_alt)
end

function make_resource_tag(ν::Int, η::String, ω::String)
    my_rts = ResourceTagSetup(ν, η, ω)
    threads = calc_threads(my_rts)
    name = rt_name(my_rts)
    wallclock = clock_to_str(ν)
    prefix = "<freInclude name=\"$name\">\n  <resources jobWallclock=\"$wallclock\">\n\n"
    suffix = "  </resources>\n</freInclude>\n\n"
    my_rt = ResourceTag(my_rts, threads, name, wallclock, prefix, suffix)
    c3 = make_cluster(my_rt, "c3")
    c4 = make_cluster(my_rt, "c4")
    c3_atm_layout, c3_alt_atm_layouts, c4_atm_layout, c4_alt_atm_layouts = match_layouts(c3.possible_atm_layouts, c4.possible_atm_layouts)
    atm_io_layout = make_io_layout(c3_atm_layout)
    ice_io_layout = make_io_layout(c3.ice_layout)
end

make_resource_tag(3, "off", "off")

cluster = "c3"
atm_layout_string = "3,12"
atm_io_layout_string = "1,4"
blank_rt_padding = 24
blank_ht_padding = 18
atm_layout_padding = 8 - length(atm_layout_string)
atm_io_layout_padding = 5 - length(atm_io_layout_string)
println(" "^4 * "<site=\"$cluster\">")
println(" "^6 * "<lnd " * " "^blank_rt_padding * " layout=\"$atm_layout_string\"" * " "^atm_layout_padding * " io_layout=\"$atm_io_layout_string\"" * " "^atm_io_layout_padding * " "^blank_ht_padding * " />")

println(clock_to_str(6))
println(clock_to_str(3))
println(clock_to_str(192))

println(atm_factors(96))
println(ice_factors(96))


#def atm_factors(n):
#    return list(set((i, int(n//i)) for i in range(2, int(n**0.5) + 1) if n % i == 0 and i < n and ((n//(i**2) < 5 and n//(i**2) > 1) or i == 3)))
#
#def ice_factors(n):
#    return list(set((i, int(n//i)) for i in range(2, int(n**0.5) + 1) if i == 3 ))
#

#def blanks(incoming_integer):
#    return ' ' * incoming_integer
#
#class Cluster():
#    def __init__(self, parent, cluster):
#        self.parent = parent
#        self.cluster = cluster
#        if "c3" in self.cluster:
#            self.cores_per_node = 32
#        elif "c4" in self.cluster:
#            self.cores_per_node = 36
#        else:
#            raise NameError
#
#        self.ranks = self.cores_per_node * self.parent.nodes
#        self.layout_cores = self.ranks / 6
#        self.possible_atm_layouts = atm_factors(self.layout_cores)
#        self.ice_layout = ice_factors(self.ranks)[0]
#
#    def make_io_layouts(self):
#        self.atm_io_layout = (1, determine_io(self.atm_layout[1]))
#        self.ice_io_layout = (1, determine_io(self.ice_layout[0]))
#
#    def make_layout_strings(self):
#        self.atm_layout_string = f'{str(self.atm_layout[0])},{str(self.atm_layout[1])}'
#        self.ice_layout_string = f'{str(self.ice_layout[1])},{str(self.ice_layout[0])}'
#        self.atm_io_layout_string = f'{str(self.atm_io_layout[0])},{str(self.atm_io_layout[1])}'
#        self.ice_io_layout_string = f'{str(self.ice_io_layout[0])},{str(self.ice_io_layout[1])}'
#
#    def resolve_layouts(self):
#        self.make_io_layouts()
#        self.make_layout_strings()
#
#    def write_freinclude(self):
#        atm_ranks_padding = 4 - len(str(self.ranks))
#        ocn_ranks_padding = 3
#        blank_ranks_padding = 12
#        blank_threads_padding = 11
#        atm_layout_padding = 8 - len(self.atm_layout_string)
#        ice_layout_padding = 8 - len(self.ice_layout_string)
#        atm_io_layout_padding = 5 - len(self.atm_io_layout_string)
#        ice_io_layout_padding = 5 - len(self.ice_io_layout_string)
#        if 'on' in self.parent.ht:
#            ht_padding = 1
#        else:
#            ht_padding = 0
#        blank_ht_padding = 17
#        string_lines = []
#        string_lines.append(f'{blanks(4)}<site="{self.cluster}">')
#        string_lines.append(f'{blanks(6)}<atm ranks="{str(self.ranks)}"{blanks(atm_ranks_padding)} threads="{str(self.parent.threads)}" layout="{self.atm_layout_string}"{blanks(atm_layout_padding)} io_layout="{self.atm_io_layout_string}"{blanks(atm_io_layout_padding)} hyperthread="{self.parent.ht}"{blanks(ht_padding)} />')
#        string_lines.append(f'{blanks(6)}<lnd {blanks(blank_ranks_padding)} {blanks(blank_threads_padding)} layout="{self.atm_layout_string}"{blanks(atm_layout_padding)} io_layout="{self.atm_io_layout_string}"{blanks(atm_io_layout_padding)} {blanks(blank_ht_padding)} />')
#        string_lines.append(f'{blanks(6)}<ocn ranks="0"{blanks(ocn_ranks_padding)} threads="0" layout="{self.ice_layout_string}"{blanks(ice_layout_padding)} io_layout="{self.ice_io_layout_string}"{blanks(ice_io_layout_padding)} hyperthread="off" />')
#        string_lines.append(f'{blanks(6)}<ice {blanks(blank_ranks_padding)} {blanks(blank_threads_padding)} layout="{self.ice_layout_string}"{blanks(ice_layout_padding)} io_layout="{self.ice_io_layout_string}"{blanks(ice_io_layout_padding)} {blanks(blank_ht_padding)} />')
#        string_lines.append(f'{blanks(4)}<site/>\n')
#        return '\n'.join(string_lines)
#
#class ResourceTag():
#    def __init__(self, nodes=3, ht='off', omp='off'):
#        self.nodes = nodes
#        self.threads = 1
#        self.ht = ht
#        if 'on' in self.ht:
#            self.threads *= 2
#        self.omp = omp
#        if 'on' in self.omp:
#            self.threads *= 2
#        self.name = f'{str(self.nodes)}nodes_ht_{self.ht}_omp_{self.omp}'
#        self.freinclude_block = ''
#        self.wallclock = self.clock_to_str()
#        self.c3 = Cluster(self, "c3")
#        self.c4 = Cluster(self, "c4")
#        self.matched_atm_layouts = self.match_layouts()
#
#    def clock_to_str(self):
#        seconds = fld(540 * 60, nodes)
#        hours = fld(seconds, 3600)
#        minutes = mod(fld(seconds, 60), 60)
#        if (minutes < 3 && hours == 0)
#            minutes = 3
#        return "$lpad(hours,2,0):$lpad(minutes,2,0):00"
#
#    def match_layouts(self):
#        layout_diff = 99999
#        c3idx = 0
#        c4idx = 0
#        for c3i, c3l in enumerate(self.c3.possible_atm_layouts):
#            for c4i, c4l in enumerate(self.c4.possible_atm_layouts):
#                this_diff = abs(c3l[0] - c4l[0]) + abs(c3l[1] - c4l[1])
#                if this_diff < layout_diff:
#                    layout_diff = this_diff
#                    c3idx = c3i
#                    c4idx = c4i
#        self.c3.atm_layout = self.c3.possible_atm_layouts[c3idx]
#        self.c3.alt_atm_layouts = list(set(self.c3.possible_atm_layouts) - set(self.c3.atm_layout))
#        self.c4.atm_layout = self.c4.possible_atm_layouts[c4idx]
#        self.c4.alt_atm_layouts = list(set(self.c4.possible_atm_layouts) - set(self.c4.atm_layout))
#
#    def resolve_cluster_layouts(self):
#        self.c3.resolve_layouts()
#        self.c4.resolve_layouts()
function calc_threads(σ::ResourceTagSetup)
    ht = σ.ht == "off" ? 1 : σ.ht == "on" ? 2 : throw(DomainError(σ.ht, "argument must be \"off\" or \"on\""))
    omp = σ.omp == "off" ? 1 : σ.omp == "on" ? 2 : throw(DomainError(σ.omp, "argument must be \"off\" or \"on\""))
    ht * omp
end
#
#    def write_prefix(self):
#        string_lines = []
#        string_lines.append(f'<freInclude name="{self.name}">')
#        string_lines.append(f'  <resources jobWallclock="{self.wallclock}">\n')
#        self.freinclude_block += '\n'.join(string_lines)
#        return
#
#    def write_suffix(self):
#        string_lines = []
#        string_lines.append('  </resources>')
#        string_lines.append('</freInclude>')
#        string_lines.append('\n')
#        self.freinclude_block += '\n'.join(string_lines)
#        return
#
#    def write_all(self):
#        self.write_prefix()
#        self.freinclude_block += self.c3.write_freinclude()
#        self.freinclude_block += self.c4.write_freinclude()
#        self.write_suffix()
#
#    def resolve_and_write(self):
#        self.resolve_cluster_layouts()
#        self.write_all()
#
#def write_full(node_count_list=[3,6,9,12,24,48,96,192], ht_options=['off', 'on'], omp_options=['off', 'on'], outfile='test_file.xml'):
#    for nd_count in node_count_list:
#        for ht_opt in ht_options:
#            for omp_opt in omp_options:
#                my_rt = ResourceTag(nodes=nd_count, ht=ht_opt, omp=omp_opt)
#                my_rt.resolve_and_write()
#                with open(outfile, 'a+') as fh:
#                    fh.write(my_rt.freinclude_block)
#
#if __name__ == '__main__':
#    write_full()
