rm(list = ls())
library(RevoScaleR)

xdf_path <- "C:/Users/adminuser/Downloads/XDF1M"

years <- 2015:2015
months <- 1:12
colors <- c("yellow")
all_months_files <- expand.grid(year = years, month = months, color = colors)

# create a name of the i-th file in the training/test set
get_filename <- function(grid, i, xdf_path) {
    month <- grid$month[i]
    month0 <- sprintf("%02d", month)
    year <- grid$year[i]
    color <- grid$color[i]
    return(file.path(xdf_path, paste(color, "_tripdata_", year, "-", month0, ".xdf", sep = "")))
}

n_cores <- 6
grid <- all_months_files
nrows <- nrow(grid)
block_vec <- rep(1:floor(nrows / 2), each = 2)
if (nrows > length(block_vec))
    block_vec[length(block_vec) + 1] <- ceiling(nrows / 2)
grid$block <- block_vec


n_pairs <- length(unique(grid$block[grid$block != -1]))
chunk_size <- ceiling(n_pairs / n_cores)

# merge two xdf files. The merged file is written in the first file
merge_blocks <- function(i) # i - vector of ids of the pairs to be merged
    {
        # get ids of files to be merged
        pair <- which(grid$block == i)

        # actual merge    
        tmp_file <- file.path(xdf_path, paste("tmp", i, ".xdf", sep = ""))
        if (length(pair) == 2) {
            file1 <- get_filename(grid, pair[1], xdf_path)
            file2 <- get_filename(grid, pair[2], xdf_path)
            rxMerge(file1, file2, tmp_file, type = "union", overwrite = TRUE)
        }
}

rxOptions(numCoresToUse = n_cores)
rxSetComputeContext("localpar") # use the local parallel compute context
rxExec(merge_blocks, elemArgs = 1:n_pairs, taskChunkSize = chunk_size, execObjects = c("grid", "get_filename", "xdf_path"))

sum(grid$block != -1)

# reassign block numbers for merge at the next (upper) level
new_block <- 0
if (floor(n_pairs / 2) == ceiling(n_pairs / 2)) n_pairs_merge <- n_pairs else n_pairs_merge <- n_pairs - 1 # odd number of file to merge. The last 'pair' has only one file

for (i in seq(1, n_pairs_merge, 2)) {
    new_block <- new_block + 1
    min_ind1 <- min(which(grid$block == i))
    min_ind2 <- min(which(grid$block == i + 1))

    # remove old block number
    grid$block[grid$block == i] <- -1
    grid$block[grid$block == i + 1] <- -1

    # assign new block number only to first files in pairs (those were the files that the pair was merged in)
    grid$block[min_ind1] <- new_block
    grid$block[min_ind2] <- new_block
}

if (n_pairs_merge < n_pairs) {
    # there was odd number of files to merge. The last file was not merged
    new_block <- new_block + 1
    min_ind <- min(which(grid$block == n_pairs))
    grid$block[grid$block == n_pairs] <- -1
    grid$block[min_ind] <- ceiling(n_pairs / 2)
}
