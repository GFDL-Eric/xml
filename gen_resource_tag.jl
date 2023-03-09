abstract type Layout end
abstract type BlockLine end
struct 🌩️Layout <: Layout
    layout::Tuple{Int,Int}
    🌩️Layout(layout) = new(layout)
end

struct 🧊Layout <: Layout
    layout::Tuple{Int,Int}
    🧊Layout(layout) = new(layout)
end

struct IOLayout <: Layout
    layout::Tuple{Int,Int}
    IOLayout(layout) = new(layout)
end

struct 🌩️Line <: BlockLine
    prefix::Int
    ranks::String
    🧵s::String
    🌩️::String
    🌩️_io::String
    ht::String
    function 🌩️Line(int_ranks::Int, int_🧵s::Int, layouts::Dict{String,String}, ht::String; prefix::Int = 4)
        ranks = repr(int_ranks)
        🧵s = repr(int_🧵s)
        🌩️ = layouts["🌩️"]
        🌩️_io = layouts["🌩️_io"]
        new(prefix, ranks, 🧵s, 🌩️, 🌩️_io, ht)
    end
end

struct 🌳Line <: BlockLine
    prefix::Int
    🌩️::String
    🌩️_io::String
    function 🌳Line(layouts::Dict{String,String}; prefix::Int = 4)
        🌩️ = layouts["🌩️"]
        🌩️_io = layouts["🌩️_io"]
        new(prefix, 🌩️, 🌩️_io)
    end
end

struct 🌊Line <: BlockLine
    prefix::Int
    🧊::String
    🧊_io::String
    function 🌊Line(layouts::Dict{String,String}; prefix::Int = 4)
        🧊 = layouts["🧊"]
        🧊_io = layouts["🧊_io"]
        new(prefix, 🧊, 🧊_io)
    end
end

struct 🧊Line <: BlockLine
    prefix::Int
    🧊::String
    🧊_io::String
    function 🧊Line(layouts::Dict{String,String}; prefix::Int = 4)
        🧊 = layouts["🧊"]
        🧊_io = layouts["🧊_io"]
        new(prefix, 🧊, 🧊_io)
    end
end

struct ResourceTagSetup
    nodes::Int
    ht::String
    omp::String
    ResourceTagSetup(nodes, ht, omp) = new(nodes, ht, omp)
end

struct ResourceTag
    setup::ResourceTagSetup
    🧵s::Int
    name::String
    wall🕛::String
    prefix::String
    suffix::String
    ResourceTag(setup, 🧵s, name, wall🕛, prefix, suffix) = new(setup, 🧵s, name, wall🕛, prefix, suffix)
end

struct Cluster
    ρ::ResourceTag
    κ::String
    cores_per_node::Int
    ranks::Int
    layout_cores::Int
    possible_🌩️_layouts::Array{🌩️Layout}
    🧊_layout::🧊Layout
    Cluster(ρ, κ, cores_per_node, ranks, layout_cores, possible_🌩️_layouts, 🧊_layout) = new(ρ, κ, cores_per_node, ranks, layout_cores, possible_🌩️_layouts, 🧊_layout)
end

determine_io(α::🌩️Layout) = mod(α.layout[2],4) == 0 ? 4 : mod(α.layout[2],3) == 0 ? 3 : mod(α.layout[2],2) == 0 ? 2 : 1
determine_io(ι::🧊Layout) = mod(ι.layout[1],4) == 0 ? 4 : mod(ι.layout[1],3) == 0 ? 3 : mod(ι.layout[1],2) == 0 ? 2 : 1

function calc_🧵s(σ::ResourceTagSetup)
    ht = σ.ht == "off" ? 1 : σ.ht == "on" ? 2 : throw(DomainError(σ.ht, "argument must be \"off\" or \"on\""))
    omp = σ.omp == "off" ? 1 : σ.omp == "on" ? 2 : throw(DomainError(σ.omp, "argument must be \"off\" or \"on\""))
    ht * omp
end

🌩️_factors(χ::Int) = unique([round.(Int, (i,fld(χ,i))) for i=2:floor(sqrt(χ)) if mod(χ,i) == 0 && fld(χ,i) < 25 && i < 25 && (fld(χ,i^2) < 5 || i == 3)])
🧊_factors(χ::Int) = unique([round.(Int, (i,fld(χ,i))) for i=2:floor(sqrt(χ)) if i == 3])

rt_name(σ::ResourceTagSetup) = "$(repr(σ.nodes))nodes_ht_$(σ.ht)_omp_$(σ.omp)"

function 🕛_to_str(ν::Int)
    seconds = fld(540 * 60, ν)
    hours = fld(seconds, 3600)
    minutes = hours == 0 ? max(3,mod(fld(seconds, 60), 60)) : mod(fld(seconds, 60), 60)
    "$(lpad(repr(hours),2,repr(0))):$(lpad(repr(minutes),2,repr(0))):00"
end

function make_cluster(ρ::ResourceTag, κ::String)
    cores_per_node = κ == "c3" ? 32 : κ == "c4" ? 36 : throw(DomainError(κ, "cluster not recognized, current clusters are \"c3\" and \"c4\"."))
    ranks = cores_per_node * ρ.setup.nodes
    layout_cores = div(ranks, 6)
    possible_🌩️_layouts = [🌩️Layout(🌩️_fact) for 🌩️_fact in 🌩️_factors(layout_cores)]
    🧊_layout = 🧊Layout(🧊_factors(ranks)[1])
    my_cluster = Cluster(ρ, κ, cores_per_node, ranks, layout_cores, possible_🌩️_layouts, 🧊_layout)
end

make_io_layout(λ::Layout) = IOLayout((1, determine_io(λ)))

make_layout_string(λ::Layout) = "$(λ.layout[1]),$(λ.layout[2])"

function match_layouts(γ::Array{🌩️Layout,1}, ϵ::Array{🌩️Layout,1})
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

function make_cluster_layouts(χ::Cluster, α::🌩️Layout)
    🌩️_layout = α
    🧊_layout = χ.🧊_layout
    🌩️_io_layout = make_io_layout(🌩️_layout)
    🧊_io_layout = make_io_layout(🧊_layout)
    proper_🧊_layout = 🧊Layout(reverse(🧊_layout.layout))
    layout_strings = Dict(x => make_layout_string(y) for (x,y) in zip(["🌩️","🧊","🌩️_io","🧊_io"],
                    [🌩️_layout, proper_🧊_layout, 🌩️_io_layout, 🧊_io_layout]))
end

function padding(ranks::Int, ht_bool::String, layouts::Dict{String,String}; 🌊_ranks::Int = 4,
                 bl_ranks::Int = 14, bl_🧵s::Int = 12, bl_ht::Int = 18, 🌩️_base::Int = 5,
                 lay_base::Int = 8, io_lay_base::Int = 5)
    🌩️_ranks = 🌩️_base - length(repr(ranks))
    🌩️ = lay_base - length(layouts["🌩️"])
    🧊 = lay_base - length(layouts["🧊"])
    🌩️_io = io_lay_base - length(layouts["🌩️_io"])
    🧊_io = io_lay_base - length(layouts["🧊_io"])
    if "$ht_bool" == "on"
        ht = 2
    else
        ht = 1
    end
    return Dict("🌩️_ranks" => 🌩️_ranks, "🌊_ranks" => 🌊_ranks, "🌩️" => 🌩️, "🧊" => 🧊,
                "🌩️_io" => 🌩️_io, "🧊_io" => 🧊_io, "ht" => ht, "bl_ht" => bl_ht,
                "bl_ranks" => bl_ranks, "bl_🧵s" => bl_🧵s)
end

function add_bl(ι::String, π::Int)
    return ι * " "^π
end

function write_block_line(α::🌩️Line, pd::Dict{String,Int})
    prefix = " "^α.prefix
    rank = add_bl("<atm ranks=\"$(α.ranks)\"", pd["🌩️_ranks"])
    🧵 = add_bl("threads=\"$(α.🧵s)\"", 1)
    layout = add_bl("layout=\"$(α.🌩️)\"", pd["🌩️"])
    io_layout = add_bl("io_layout=\"$(α.🌩️_io)\"", pd["🌩️_io"])
    ht = add_bl("hyperthread=\"$(α.ht)\"", pd["ht"])
    suffix = "/>\n"
    return "$prefix" * "$rank" * "$🧵" * "$layout" * "$io_layout" * "$ht" * "$suffix"
end

function write_block_line(λ::🌳Line, pd::Dict{String,Int})
    prefix = " "^λ.prefix
    rank = add_bl("<lnd", pd["bl_ranks"])
    🧵 = add_bl("", pd["bl_🧵s"])
    layout = add_bl("layout=\"$(λ.🌩️)\"", pd["🌩️"])
    io_layout = add_bl("io_layout=\"$(λ.🌩️_io)\"", pd["🌩️_io"])
    ht = add_bl("", pd["bl_ht"])
    suffix = "/>\n"
    return "$prefix" * "$rank" * "$🧵" * "$layout" * "$io_layout" * "$ht" * "$suffix"
end

function write_block_line(Ο::🌊Line, pd::Dict{String,Int})
    prefix = " "^Ο.prefix
    rank = add_bl("<ocn ranks=\"0\"", pd["🌊_ranks"])
    🧵 = add_bl("threads=\"0\"", 1)
    layout = add_bl("layout=\"$(Ο.🧊)\"", pd["🧊"])
    io_layout = add_bl("io_layout=\"$(Ο.🧊_io)\"", pd["🧊_io"])
    ht = add_bl("hyperthread=\"off\"", 1)
    suffix = "/>\n"
    return "$prefix" * "$rank" * "$🧵" * "$layout" * "$io_layout" * "$ht" * "$suffix"
end

function write_block_line(ι::🧊Line, pd::Dict{String,Int})
    prefix = " "^ι.prefix
    rank = add_bl("<ice", pd["bl_ranks"])
    🧵 = add_bl("", pd["bl_🧵s"])
    layout = add_bl("layout=\"$(ι.🧊)\"", pd["🧊"])
    io_layout = add_bl("io_layout=\"$(ι.🧊_io)\"", pd["🧊_io"])
    ht = add_bl("", pd["bl_ht"])
    suffix = "/>\n"
    return "$prefix" * "$rank" * "$🧵" * "$layout" * "$io_layout" * "$ht" * "$suffix"
end

function write_cluster_block(χ::String, ω::String, β::Dict{String,<:BlockLine}, pd::Dict{String,Int})
    resource_l = " "^2 * "<resources site=\"$χ\" jobWallclock=\"$ω\">\n"
    🌩️_l = write_block_line(β["🌩️"], pd)
    🌳_l = write_block_line(β["🌳"], pd)
    🌊_l = write_block_line(β["🌊"], pd)
    🧊_l = write_block_line(β["🧊"], pd)
    resource_end = " "^2 * "</resources>\n"
    return "$resource_l" * "$🌩️_l" * "$🌳_l" * "$🌊_l" * "$🧊_l" * "$resource_end"
end

function make_block_lines(ρ::Int, τ::Int, λ::Dict{String, String}, η::String)
    return Dict{String,BlockLine}("🌩️" => 🌩️Line(ρ, τ, λ, η), "🌳" => 🌳Line(λ),
                                  "🌊" => 🌊Line(λ), "🧊" => 🧊Line(λ))
end

function make_resource_tag(ν::Int, η::String, ω::String)
    my_rts = ResourceTagSetup(ν, η, ω)
    🧵s = calc_🧵s(my_rts)
    name = rt_name(my_rts)
    wall🕛 = 🕛_to_str(ν)
    prefix = "<freInclude name=\"$name\">\n"
    suffix = "</freInclude>\n\n"
    my_rt = ResourceTag(my_rts, 🧵s, name, wall🕛, prefix, suffix)
    c3 = make_cluster(my_rt, "c3")
    c4 = make_cluster(my_rt, "c4")
    c3_🌩️_layout, c3_alt_🌩️_layouts, c4_🌩️_layout, c4_alt_🌩️_layouts = match_layouts(c3.possible_🌩️_layouts, c4.possible_🌩️_layouts)
    c3_layouts = make_cluster_layouts(c3, c3_🌩️_layout)
    c4_layouts = make_cluster_layouts(c4, c4_🌩️_layout)
    c3_pd = padding(c3.ranks, my_rts.ht, c3_layouts)
    c4_pd = padding(c4.ranks, my_rts.ht, c4_layouts)
    c3_Lines = make_block_lines(c3.ranks, 🧵s, c3_layouts, my_rt.setup.ht)
    c4_Lines = make_block_lines(c4.ranks, 🧵s, c4_layouts, my_rt.setup.ht)
    c3_block = write_cluster_block("ncrc3", wall🕛, c3_Lines, c3_pd)
    c4_block = write_cluster_block("ncrc4", wall🕛, c4_Lines, c4_pd)
    full_block = "$prefix" * "$c3_block" * "$c4_block" * "$suffix"
end

function write_full(;node_count_list::Array{Int,1} = [3,6,9,12,24,48,72],
                    ht_options::Array{String, 1} = ["off", "on"],
                    omp_options::Array{String, 1} = ["off", "on"],
                    outfile="julia_test_file.xml")
    for nd_count in node_count_list
        for ht_opt in ht_options
            for omp_opt in omp_options
                open(outfile, "a+") do my_f
                    write(my_f, make_resource_tag(nd_count, ht_opt, omp_opt))
                end
            end
        end
    end
end

write_full()
