#==============================================================================
    data wrangling tutorial I
        basic dataframe and wrangle excercises
        overview presentation video part 1

    project:    data-work
    author:     merlinr@
 =============================================================================#

using DesertIslandDisk, Markdown
using CSV, DataFrames, Pipe, Dates


# read / process / visualize #
# ========================== #
#=
recommended packages:
CSV, Parquet, JSON, XLSX, ExcelReaders, Taro, PDFIO
DataFrames, DataFramesMeta, Query, JuliaDB, JuliaDBMeta
Plots, StatsPlots, Makie, StatsMakie, Vegalite, Gadfly, Gaston, UnicodePlots
=#

# CSV.jl features #
# =============== #
md"""
## by default it:

* auto-delimiter detection
* skip leading/trail rows
* parse missing values
* parse date/time, boolean
* normalize col names
* select/drop cols on read
* define column types
* transpose
"""


# data cleaning #
# ============= #

# example data
df = DataFrame(CSV.File("data/youth_suicide.csv"))
dfcounty = DataFrame(CSV.File("data/youth_suicide.csv", drop=[2:18...]))


describe(df)


# SELECT
select(df, Not(), Between(), AsTable() )


# selecting and removing cols
select!(df, 1, 7:11)

select(df, [1, 3:7..., 19])

select(df,Not(["County", "Total (2012)", "Total (2011)", "Total (2009)"]))


# renaming
rename!(dfcounty, "County" => "my_nice_county", "Total (2008-2012)" => "all_muh_total")


# remove rows with missing data
dropmissing!(df)

dropmissing!(df, Between(3, 5))


# TIDY DATA #
# ========= #

md"""
# TIDY IS:
* every variable is a column
* every observation is a row
* every type of observed unit forms a table
"""

# tidy examples #
# ============= #

df = DataFrame(CSV.File("data/zhvi.csv"))
select!(df, :RegionName, 294:297)

# stack all grouping columns into rows
sdf = stack(df, Not(:RegionName); variable_name = :Date)

# refer to column name
sdf.ReportDate

describe(sdf, :eltype)

#=  need to retype that categorical value
│ Row │ variable   │ eltype                          │
│     │ Symbol     │ DataType                        │
├─────┼────────────┼─────────────────────────────────┤
│ 1   │ RegionName │ String                          │
│ 2   │ ReportDate │ CategoricalValue{String,UInt32} │
│ 3   │ value      │ Float64                         │
=#

# date handling #
# ============= #

sdf.ReportDate = [Date(get(x)) for x in sdf.ReportDate]
# 3660-element Array{Date,1}:
#  2020-01-31
#  2020-01-31
#  2020-01-31
# very nice

# turn back to wide format
df2 = unstack(sdf, :ReportDate, :value)

# retype from Union{Missing, T} if there
disallowmissing!(df2, 2:5)

# filter is part of Base - df argument is at the end
filter(:RegionName => ==("Abilene, TX"), df)

# filter from pattern regex match
filter(:RegionName => x -> occursin(r"Abil", x), df)

filter(:RegionName => x -> contains(x, "Abil"), df)

filter(2 => >(400_000), df)

sort(df, 4, rev=true)

# transforms #
# ========== #

# adding columns based on a transform
# ByRow applies a function to value in each row
transform(df, :RegionName => ByRow(length) => :RegionNameLength)

# more useful: extract the state code pattern from RegionName string
transform(df,
    :RegionName => ByRow(x -> begin
        m = match(r", (..)", x)
        if typeof(m) == Nothing ""
        else m[1]
    end end) => :State)

# alternately with function definiton, one line
tdf = transform(df, :RegionName => ByRow(
    function (x) m = match(r", (..)", x); if typeof(m) == Nothing "" else m[1] end end) => :State)


# grouping by #
# =========== #
# grouping allows aggregates on groups of a categorical or date column

# returns grouped dataframes
groupby(sdf, :ReportDate)

using Statistics

# agg over each group, union the groups back up
combine(groupby(sdf, :ReportDate), :value => median => :P50)

combine(groupby(sdf, :ReportDate), :value => x -> quantile(x, [0.25:0.25:0.75...]) => (:P25, :P50, :P75)
#=
4×2 DataFrame
│ Row │ ReportDate │ P50      │
│     │ Date       │ Float64  │
├─────┼────────────┼──────────┤
│ 1   │ 2020-01-31 │ 155163.0 │
│ 2   │ 2020-02-29 │ 155726.0 │
│ 3   │ 2020-03-31 │ 156410.0 │
│ 4   │ 2020-04-30 │ 156411.0 │
=#

# almost there, must explode final column
combine(groupby(sdf, :ReportDate), :value => x -> quantile(x, [0.25:0.25:0.75...]) => [:P25 :P50 :P75])
#=
4×2 DataFrame
│ Row │ ReportDate │ value_function                                     │
│     │ Date       │ Pair{Array{Float64,1},Array{Symbol,2}}             │
├─────┼────────────┼────────────────────────────────────────────────────┤
│ 1   │ 2020-01-31 │ [117126.0, 155163.0, 2.23714e5]=>[:P25 :P50 :P75]  │
│ 2   │ 2020-02-29 │ [117609.0, 155726.0, 224787.0]=>[:P25 :P50 :P75]   │
│ 3   │ 2020-03-31 │ [1.17992e5, 156410.0, 2.25734e5]=>[:P25 :P50 :P75] │
│ 4   │ 2020-04-30 │ [1.1849e5, 156411.0, 226593.0]=>[:P25 :P50 :P75]   │
=#


# pipe syntax #
# =========== #

@pipe sdf |>
    groupby(_, :ReportDate) |>
    combine(_, :value => mean => :avg)


# pipe syntax is nice because of the incremental stages of transform are easier to read

# syntax flows from inside-out of the function calls


# joins #
# ===== #

country = DataFrame(Name = ["United States"; "Zoldova"; "Aazonia"])
states = DataFrame(StateCode = ["CA"; "TX"; "GA"])

# joining is easy
innerjoin(df, country, on = [:RegionName => :Name])

innerjoin(tdf, states, on = [:State => :StateCode])

@pipe rightjoin(df, country, on = [:RegionName => :Name]) |>
    sort!(_, :RegionName)
#=    
3×5 DataFrame
│ Row │ RegionName    │ 2020-01-31 │ 2020-02-29 │ 2020-03-31 │ 2020-04-30 │
│     │ String        │ Float64?   │ Float64?   │ Float64?   │ Float64?   │
├─────┼───────────────┼────────────┼────────────┼────────────┼────────────┤
│ 1   │ Aazonia       │ missing    │ missing    │ missing    │ missing    │
│ 2   │ United States │ 247060.0   │ 248046.0   │ 249140.0   │ 250271.0   │
│ 3   │ Zoldova       │ missing    │ missing    │ missing    │ missing    │
=#


@pipe first(tdf, 6) |>
    antijoin(_, states, on = [:State => :StateCode])

#=
│ Row │ RegionName       │ 2020-01-31 │ 2020-02-29 │ 2020-03-31 │ 2020-04-30 │ State    │
│     │ String           │ Float64    │ Float64    │ Float64    │ Float64    │ Abstrac… │
├─────┼──────────────────┼────────────┼────────────┼────────────┼────────────┼──────────┤
│ 1   │ United States    │ 247060.0   │ 248046.0   │ 249140.0   │ 250271.0   │          │
│ 2   │ New York, NY     │ 485111.0   │ 486070.0   │ 486979.0   │ 488002.0   │ NY       │
│ 3   │ Chicago, IL      │ 242376.0   │ 242743.0   │ 243262.0   │ 243627.0   │ IL       │
│ 4   │ Philadelphia, PA │ 252614.0   │ 253547.0   │ 254232.0   │ 255300.0   │ PA       │
=#


# TX and GA no longer members
# nice!

