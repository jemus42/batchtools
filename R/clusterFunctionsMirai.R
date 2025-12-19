#' @title ClusterFunctions for mirai (External Daemons)
#'
#' @description
#' Jobs are dispatched to mirai daemons for asynchronous evaluation.
#' This uses mirai's native task dispatch mechanism with optimal FIFO scheduling
#' via the dispatcher.
#'
#' With this approach, you manage the daemon lifecycle yourself by calling
#' \code{mirai::daemons()} before submitting jobs. This gives you full control
#' over daemon configuration including remote SSH connections and TLS encryption.
#'
#' This approach provides:
#' \itemize{
#'   \item Native SSH and TLS support for remote execution
#'   \item Optimal task scheduling via mirai's dispatcher
#'   \item Support for HPC cluster resource managers (Slurm, SGE, PBS, LSF)
#'   \item Task cancellation support via \code{killJobs()}
#'   \item Full control over daemon configuration
#' }
#'
#' @note
#' \strong{Important}: The mirai daemons are tied to your R session. If you close R,
#' the dispatcher and all daemons will exit, and any running jobs will be lost.
#'
#' You must set up daemons before submitting jobs. For local execution:
#' \preformatted{
#' mirai::daemons(n = 4)
#' }
#' For remote execution via SSH:
#' \preformatted{
#' mirai::daemons(
#'   url = mirai::host_url(tls = TRUE),
#'   remote = mirai::ssh_config(c("ssh://node1", "ssh://node2"))
#' )
#' }
#'
#' @param .compute [\code{character(1)}]\cr
#'   The mirai compute profile to use. Default is \code{NULL} which uses the
#'   \dQuote{default} compute profile. Use different profiles to separate
#'   local and remote daemon pools.
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
#' @examples
#' \dontrun{
#' # Set up local daemons first
#' mirai::daemons(n = 4)
#'
#' # Create registry with mirai cluster functions
#' reg = makeRegistry(file.dir = NA)
#' reg$cluster.functions = makeClusterFunctionsMirai()
#'
#' # Submit jobs
#' batchMap(identity, 1:10, reg = reg)
#' submitJobs(reg = reg)
#' waitForJobs(reg = reg)
#'
#' # When done, reset daemons
#' mirai::daemons(0)
#' }
makeClusterFunctionsMirai = function(.compute = NULL, fs.latency = 0) {
  if (!requireNamespace("mirai", quietly = TRUE)) {
    stop("Package 'mirai' is required for makeClusterFunctionsMirai. Please install it.")
  }

  # Manager to track mirai objects
  manager = MiraiManager$new(.compute)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    batch.id = manager$spawn(reg, jc)
    makeSubmitJobResult(status = 0L, batch.id = batch.id)
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    manager$kill(batch.id)
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    manager$list()
  }

  makeClusterFunctions(
    name = "Mirai",
    submitJob = submitJob,
    killJob = killJob,
    listJobsRunning = listJobsRunning,
    store.job.collection = TRUE,
    fs.latency = fs.latency,
    hooks = list(pre.sync = function(reg, fns) manager$collect())
  )
}


MiraiManager = R6Class("MiraiManager",
  cloneable = FALSE,
  public = list(
    .compute = NULL,
    mirais = NULL,  # Environment storing mirai objects by batch.id

    initialize = function(.compute = NULL) {
      self$.compute = .compute
      self$mirais = new.env(parent = emptyenv())
    },

    spawn = function(reg, jc) {
      # Check that daemons are set up
      if (!mirai::daemons_set(.compute = self$.compute)) {
        stop("mirai daemons are not set up. Call mirai::daemons() first.")
      }

      # Create mirai task to execute doJobCollection
      # We pass the file paths since the job collection is serialized to disk
      m = mirai::mirai(
        {
          batchtools::doJobCollection(jc_uri, jc_log)
        },
        jc_uri = jc$uri,
        jc_log = jc$log.file,
        .compute = self$.compute
      )

      # Use the job hash as batch.id (more meaningful than mirai's internal ID)
      batch.id = jc$job.hash

      # Store the mirai object
      self$mirais[[batch.id]] = m

      batch.id
    },

    kill = function(batch.id) {
      if (exists(batch.id, envir = self$mirais, inherits = FALSE)) {
        m = self$mirais[[batch.id]]
        mirai::stop_mirai(m)
        rm(list = batch.id, envir = self$mirais)
      }
      invisible(TRUE)
    },

    list = function() {
      self$collect()
      ls(self$mirais)
    },

    collect = function() {
      # Remove resolved mirais from our tracking
      batch.ids = ls(self$mirais)
      for (bid in batch.ids) {
        m = self$mirais[[bid]]
        if (!mirai::unresolved(m)) {
          rm(list = bid, envir = self$mirais)
        }
      }
      invisible(NULL)
    }
  )
)


#' @title ClusterFunctions for mirai (Managed Local Daemons)
#'
#' @description
#' Jobs are dispatched to mirai daemons for asynchronous evaluation.
#' Unlike \code{\link{makeClusterFunctionsMirai}}, this function manages the
#' daemon lifecycle automatically - daemons are created when the cluster
#' functions are initialized and cleaned up when the R session ends.
#'
#' This is the simpler option for local parallel execution, similar to
#' \code{\link{makeClusterFunctionsSocket}} but using mirai's more efficient
#' dispatcher for optimal task scheduling.
#'
#' @param ncpus [\code{integer(1)}]\cr
#'   Number of local daemons (parallel workers) to launch. If \code{NA} (default),
#'   the number is auto-detected based on available CPUs.
#' @param .compute [\code{character(1)}]\cr
#'   The mirai compute profile to use. Default is \code{"batchtools"} to avoid
#'   conflicts with other mirai usage in the same session.
#' @inheritParams makeClusterFunctions
#' @return [\code{\link{ClusterFunctions}}].
#' @family ClusterFunctions
#' @export
#' @examples
#' \dontrun{
#' # Create registry with managed mirai daemons
#' reg = makeRegistry(file.dir = NA)
#' reg$cluster.functions = makeClusterFunctionsMiraiLocal(ncpus = 4)
#'
#' # Submit jobs - daemons are already running
#' batchMap(identity, 1:10, reg = reg)
#' submitJobs(reg = reg)
#' waitForJobs(reg = reg)
#'
#' # Daemons are cleaned up when R exits, or manually with:
#' # mirai::daemons(0, .compute = "batchtools")
#' }
makeClusterFunctionsMiraiLocal = function(ncpus = NA_integer_, .compute = "batchtools",
                                           fs.latency = 0) {
  if (!requireNamespace("mirai", quietly = TRUE)) {
    stop("Package 'mirai' is required for makeClusterFunctionsMiraiLocal. Please install it.")
  }

  if (is.na(ncpus)) {
    ncpus = max(getOption("mc.cores", parallel::detectCores()), 1L, na.rm = TRUE)
    info("Auto-detected %i CPUs", ncpus)
  }
  ncpus = asCount(ncpus, positive = TRUE)

  # Set up daemons for this compute profile
  # Using a dedicated profile avoids conflicts with other mirai usage
  mirai::daemons(n = ncpus, .compute = .compute)

  # Manager to track mirai objects
  manager = MiraiManager$new(.compute)

  submitJob = function(reg, jc) {
    assertRegistry(reg, writeable = TRUE)
    assertClass(jc, "JobCollection")

    batch.id = manager$spawn(reg, jc)
    makeSubmitJobResult(status = 0L, batch.id = batch.id)
  }

  killJob = function(reg, batch.id) {
    assertRegistry(reg, writeable = TRUE)
    assertString(batch.id)
    manager$kill(batch.id)
  }

  listJobsRunning = function(reg) {
    assertRegistry(reg, writeable = FALSE)
    manager$list()
  }

  makeClusterFunctions(
    name = "MiraiLocal",
    submitJob = submitJob,
    killJob = killJob,
    listJobsRunning = listJobsRunning,
    store.job.collection = TRUE,
    fs.latency = fs.latency,
    hooks = list(pre.sync = function(reg, fns) manager$collect())
  )
}
