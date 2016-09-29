rm(list = ls())
library(RevoScaleR)

xdf_files <- "C:/Users/adminuser/Downloads/XDF1M"

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

# merge multiple xdf files 
merge_all_months <- function(grid) {
    # 'grid' data frame has all combinations of months, years and colors to be merged
    n_cores <- 12
    rxOptions(numCoresToUse = n_cores)
    rxSetComputeContext("localpar") # use the local parallel compute context
    # 'block' column has id of the pair to be merged
    # if pair id = -1 then the file will not be merged
    nrows <- nrow(grid)
    block_vec <- rep(1:floor(nrows / 2), each = 2)
    if (nrows > length(block_vec))
        block_vec[length(block_vec) + 1] <- ceiling(nrows / 2)
    grid$block <- block_vec

    # merge two xdf files. The merged file is written in the first file
    merge_blocks <- function(i) # i - vector of ids of the pairs to be merged
        {
        # merge each one of the input pairs
        if (length(i) > 1L)
            return(sapply(i, create_xdf))

        # get ids of files to be merged
        pair <- which(grid$block == i)

        # actual merge    
        tmp_file <- file.path(xdf_files, paste("tmp", i, ".xdf", sep = ""))
        if (length(pair) == 2) {
            file1 <- get_filename(grid, pair[1], xdf_files)
            file2 <- get_filename(grid, pair[2], xdf_files)
            rxMerge(file1, file2, tmp_file, type = "union", overwrite = TRUE)
            file.remove(file1)
            file.rename(tmp_file, file1)
        }
    }

    while (sum(grid$block != -1) > 0) {
        # while there are pairs to be merged

        # merge blocks at the same level, in parallel
        n_pairs <- length(unique(grid$block[grid$block != -1]))
        chunk_size <- ceiling(n_pairs / n_cores)
        rxExec(merge_blocks, i = rxElemArg(1:n_pairs), taskChunkSize = chunk_size,
               execObjects = c("grid", "get_filename", "xdf_files"))

        # reassign block numbers for merge at the next (upper) level
        new_block <- 0
        if (floor(n_pairs / 2) == ceiling(n_pairs / 2))
            n_pairs_merge <- n_pairs # even number of files to merge
        else
            n_pairs_merge <- n_pairs - 1 # odd number of file to merge. The last 'pair' has only one file

        if (n_pairs > 1) {

            # update block number of pairs that have two files
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
        }
        else
            grid$block <- -1 # at the current iteration there was only a single block to merge, no further merge is required
        }
}

merge_all_months(all_months_files)
