cdef extern from "keplerian.h":
	void calc_cartesian_for_ellipse(double* x,double* y, double * z, double *vx, double *vy, double *vz, double a, double e, double i, double O, double w, double M, double GM)
	void calc_keplerian_for_cartesian( double* a,  double* e,  double* i,  double* O,  double* w,  double* M, double x,double y, double z, double vx, double vy, double vz, double GM)

