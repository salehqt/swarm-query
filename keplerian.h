/*************************************************************************
 * Copyright (C) 2009-2012 by Eric Ford, Saleh Dindar &                  *
 * the Swarm-NG Development Team                                         *
 *                                                                       *
 * This program is free software; you can redistribute it and/or modify  *
 * it under the terms of the GNU General Public License as published by  *
 * the Free Software Foundation; either version 3 of the License.        *
 *                                                                       *
 * This program is distributed in the hope that it will be useful,       *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 * GNU General Public License for more details.                          *
 *                                                                       *
 * You should have received a copy of the GNU General Public License     *
 * along with this program; if not, write to the                         *
 * Free Software Foundation, Inc.,                                       *
 * 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.             *
 ************************************************************************/

#pragma once


double improve_mean_to_eccentric_annomaly_guess(double e, double M, double x);
double mean_to_eccentric_annomaly(double e,  double M);


void calc_cartesian_for_ellipse(
		double *x, double *y, double *z,
		double *vx, double *vy, double *vz,
		double a, double e, double i, double O, double w, double M,
		double GM
		);

void calc_keplerian_for_cartesian(
		double *a, double *e, double *i, double *O, double *w, double *M,
		double x, double y,  double z,
		double vx,  double vy,  double vz,
		double GM
		);
