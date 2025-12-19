#' @title ClusterFunctions for Local Parallel Execution via processx
#'
#' @description
#' Jobs are spawned asynchronously using the \pkg{processx} package.
#' Each job runs in a separate R process managed by processx.
#'
#' Unlike \code{\link{makeClusterFunctionsMulticore}} and \code{\link{makeClusterFunctionsSocket}},
#' this implementation persists job information to disk, allowing job tracking and management
#' to survive R session restarts. This makes it suitable for long-running computations where
#' you may need to close and reopen R, or where the submitting R session may crash.
#'
#' @template ncpus
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
#' @examples
#' \dontrun{
#' # Use 4 CPUs for parallel job execution
#' reg = makeRegistry(file.dir = NA)
#' reg$cluster.functions = makeClusterFunctionsProcessx(ncpus = 4)
#' }
makeClusterFunctionsProcessx = function(ncpus = NA_integer_, fs.latency = 0) {
 if (!requireNamespace("processx", quietly = TRUE)) {

    stop("Package 'processx' is required for makeClusterFunctionsProcessx. Please install it.")
  }

  if (is.na(ncpus)) {
    ncpus = max(getOption("mc.cores", parallel::detectCores()), 1L, na.rm = TRUE)
    info("Auto-detected %i CPUs", ncpus)
  }
  ncpus = asCount(ncpus, positive = TRUE)

  # Create manager instance
  p = ProcessxManager$new(ncpus)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    batch.id = p$spawn(reg, jc)
    makeSubmitJobResult(status = 0L, batch.id = batch.id)
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    p$kill(reg, batch.id)
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    p$list(reg)
  }

  makeClusterFunctions(
    name = "Processx",
    submitJob = submitJob,
    killJob = killJob,
    listJobsRunning = listJobsRunning,
    store.job.collection = TRUE,
    fs.latency = fs.latency,
    hooks = list(pre.sync = function(reg, fns) p$collect(reg))
  )
}


ProcessxManager = R6Class("ProcessxManager",
  cloneable = FALSE,
  public = list(
    ncpus = NA_integer_,
    processes = NULL,  # In-memory cache of processx handles for current session

    initialize = function(ncpus) {
      self$ncpus = ncpus
      self$processes = new.env(parent = emptyenv())
    },

    # Get the directory where we store PID files for persistence across sessions
    pid_dir = function(reg) {
      d = fs::path(reg$file.dir, "processx_pids")
      if (!fs::dir_exists(d)) {
        fs::dir_create(d, recurse = TRUE)
      }
      d
    },

    # Write PID file for a job (enables cross-session tracking)
    write_pid = function(reg, batch.id, pid) {
      pid_file = fs::path(self$pid_dir(reg), sprintf("%s.pid", batch.id))
      writeLines(as.character(pid), pid_file)
    },

    # Read PID from file
    read_pid = function(reg, batch.id) {
      pid_file = fs::path(self$pid_dir(reg), sprintf("%s.pid", batch.id))
      if (fs::file_exists(pid_file)) {
        as.integer(readLines(pid_file, n = 1L, warn = FALSE))
      } else {
        NA_integer_
      }
    },

    # Remove PID file when job is done
    remove_pid = function(reg, batch.id) {
      pid_file = fs::path(self$pid_dir(reg), sprintf("%s.pid", batch.id))
      if (fs::file_exists(pid_file)) {
        fs::file_delete(pid_file)
      }
    },

    # List all batch.ids with PID files
    list_pid_files = function(reg) {
      pid_dir = self$pid_dir(reg)
      files = fs::dir_ls(pid_dir, glob = "*.pid", type = "file")
      if (length(files) == 0L) {
        return(character(0L))
      }
      # Extract batch.id from filename (remove .pid extension)
      fs::path_ext_remove(fs::path_file(files))
    },

    # Check if a PID is still running using system call
    pid_is_alive = function(pid) {
      if (is.na(pid) || pid <= 0L) {
        return(FALSE)
      }
      # Use kill with signal 0 to check if process exists
      # This works on Unix-like systems
      tryCatch({
        # processx::process has a static method, but we need to check arbitrary PIDs
        # Use system kill -0 which returns 0 if process exists
        res = suppressWarnings(system2("kill", c("-0", as.character(pid)),
                                        stdout = FALSE, stderr = FALSE))
        res == 0L
      }, error = function(e) FALSE)
    },

    spawn = function(reg, jc) {
      # Wait until we have a free slot
      repeat {
        running = self$list(reg)
        if (length(running) < self$ncpus) {
          break
        }
        Sys.sleep(1)
      }

      # Start the job using processx
      # We use Rscript to run doJobCollection
      args = c("-e", sprintf("batchtools::doJobCollection('%s', '%s')", jc$uri, jc$log.file))

      proc = processx::process$new(
        command = file.path(R.home("bin"), "Rscript"),
        args = args,
        stdout = NULL,  # Output goes to log.file via doJobCollection
        stderr = NULL,
        cleanup = FALSE,  # Don't kill on GC - we want persistence
        supervise = FALSE  # Don't supervise - allows R session to close
      )

      pid = proc$get_pid()
      batch.id = as.character(pid)

      # Store in-memory handle
      self$processes[[batch.id]] = proc

      # Persist PID to disk for cross-session tracking
      self$write_pid(reg, batch.id, pid)

      batch.id
    },

    kill = function(reg, batch.id) {
      pid = as.integer(batch.id)

      # First try the in-memory handle if we have it
      if (exists(batch.id, envir = self$processes, inherits = FALSE)) {
        proc = self$processes[[batch.id]]
        if (inherits(proc, "process") && proc$is_alive()) {
          proc$kill()
          Sys.sleep(0.1)
          if (proc$is_alive()) {
            proc$kill(grace = 0)  # SIGKILL
          }
        }
        rm(list = batch.id, envir = self$processes)
      } else {
        # No in-memory handle (different R session), use system kill
        if (self$pid_is_alive(pid)) {
          # Try SIGTERM first
          system2("kill", c("-TERM", as.character(pid)),
                  stdout = FALSE, stderr = FALSE)
          Sys.sleep(1)
          # Then SIGKILL if still alive
          if (self$pid_is_alive(pid)) {
            system2("kill", c("-KILL", as.character(pid)),
                    stdout = FALSE, stderr = FALSE)
          }
        }
      }

      # Remove PID file
      self$remove_pid(reg, batch.id)

      invisible(TRUE)
    },

    list = function(reg) {
      # Get all batch.ids from PID files
      batch.ids = self$list_pid_files(reg)

      if (length(batch.ids) == 0L) {
        return(character(0L))
      }

      # Check which are still running
      alive = vapply(batch.ids, function(bid) {
        # First check in-memory handle
        if (exists(bid, envir = self$processes, inherits = FALSE)) {
          proc = self$processes[[bid]]
          if (inherits(proc, "process")) {
            return(proc$is_alive())
          }
        }
        # Fall back to PID check
        pid = self$read_pid(reg, bid)
        self$pid_is_alive(pid)
      }, logical(1L))

      # Clean up dead jobs
      dead = batch.ids[!alive]
      for (bid in dead) {
        self$remove_pid(reg, bid)
        if (exists(bid, envir = self$processes, inherits = FALSE)) {
          rm(list = bid, envir = self$processes)
        }
      }

      batch.ids[alive]
    },

    collect = function(reg) {
      # This is called as a pre.sync hook to clean up finished jobs
      self$list(reg)  # list() already cleans up dead jobs
      invisible(NULL)
    }
  )
)
